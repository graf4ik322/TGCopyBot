"""
Основной модуль для копирования постов из Telegram группы.
Обеспечивает идентичное копирование контента без меток пересылки.
"""

import asyncio
import logging
import os
from typing import List, Optional, Union, Dict, Any
from telethon import TelegramClient
from telethon.tl.types import (
    Message, MessageMediaPhoto, MessageMediaDocument, 
    MessageMediaWebPage, InputMediaPhoto, InputMediaDocument,
    MessageEntityTextUrl, MessageEntityUrl, MessageEntityMention,
    MessageEntityHashtag, MessageEntityBold, MessageEntityItalic,
    MessageEntityCode, MessageEntityPre, MessageEntityStrike,
    MessageEntityUnderline, MessageEntitySpoiler, MessageEntityBlockquote,
    ChannelParticipantAdmin, ChannelParticipantCreator, PeerChannel
)
from telethon.errors import FloodWaitError, PeerFloodError, MediaInvalidError
from telethon.tl import functions
# from telethon.tl.functions.channels import GetParticipantRequest - убрано, используем get_permissions
from telethon.tl.functions.messages import GetHistoryRequest
from utils import (RateLimiter, handle_flood_wait, save_last_message_id, ProgressTracker, 
                   sanitize_filename, format_file_size, MessageDeduplicator, PerformanceMonitor)
from album_handler import AlbumHandler
from message_tracker import MessageTracker


class TelegramCopier:
    """Класс для копирования сообщений между Telegram группами."""
    
    def __init__(self, client: TelegramClient, source_group_id: str, target_group_id: str,
                 rate_limiter: RateLimiter, dry_run: bool = False, resume_file: str = 'last_message_id.txt',
                 use_message_tracker: bool = True, tracker_file: str = 'copied_messages.json', 
                 add_debug_tags: bool = False, flatten_structure: bool = False):
        """
        Инициализация копировщика.
        
        Args:
            client: Авторизованный Telegram клиент
            source_group_id: ID или username исходной группы/канала
            target_group_id: ID или username целевой группы/канала
            rate_limiter: Ограничитель скорости отправки
            dry_run: Режим симуляции без реальной отправки
            resume_file: Файл для сохранения прогресса
            use_message_tracker: Использовать ли детальный трекинг сообщений
            tracker_file: Файл для хранения информации о скопированных сообщениях
            add_debug_tags: Добавлять ли debug теги к сообщениям
            flatten_structure: Превращать ли вложенность в плоскую структуру (антивложенность)
        """
        self.client = client
        self.source_group_id = source_group_id
        self.target_group_id = target_group_id
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        self.resume_file = resume_file
        self.logger = logging.getLogger('telegram_copier.copier')
        
        # Кэш для entities
        self.source_entity = None
        self.target_entity = None
        
        # Статистика
        self.copied_messages = 0
        self.failed_messages = 0
        self.skipped_messages = 0
        
        # НОВЫЕ КОМПОНЕНТЫ: Дедупликация и мониторинг
        self.deduplicator = MessageDeduplicator()
        self.performance_monitor = PerformanceMonitor()
        
        # Обработчик альбомов
        self.album_handler = AlbumHandler(client)

        # Настройки трекинга
        self.use_message_tracker = use_message_tracker
        self.add_debug_tags = add_debug_tags
        
        # НОВОЕ: Режим антивложенности
        self.flatten_structure = flatten_structure
        if self.flatten_structure:
            self.logger.info("🔄 Включен режим антивложенности - комментарии будут превращены в обычные посты")
        
        # Инициализация трекера сообщений
        if self.use_message_tracker:
            self.message_tracker = MessageTracker(tracker_file)
            self.logger.info(f"✅ Включен детальный трекинг сообщений: {tracker_file}")
        else:
            self.message_tracker = None
            self.logger.info("ℹ️ Используется простой трекинг (last_message_id.txt)")
        
        # Очистка старых хешей при инициализации
        self.deduplicator.cleanup_old_hashes()
    
    async def initialize(self) -> bool:
        """
        Инициализация entities групп/каналов и проверка доступа.
        
        Returns:
            True если инициализация успешна, False иначе
        """
        try:
            # Получаем entity исходной группы/канала
            try:
                self.source_entity = await self.client.get_entity(self.source_group_id)
                self.logger.info(f"Исходная группа/канал найдена: {self.source_entity.title}")
            except Exception as e:
                # Пробуем преобразовать ID канала в число, если это необходимо
                if self.source_group_id.startswith('-100'):
                    try:
                        numeric_id = int(self.source_group_id)
                        self.logger.info(f"Пробуем числовой ID для исходного канала: {numeric_id}")
                        self.source_entity = await self.client.get_entity(numeric_id)
                        self.logger.info(f"Исходный канал найден через числовой ID: {self.source_entity.title}")
                    except Exception as e2:
                        self.logger.error(f"Не удалось получить исходный канал через числовой ID: {e2}")
                        raise e
                else:
                    raise e
            
            # Проверяем доступ к исходной группе/каналу
            try:
                # Пытаемся получить хотя бы одно сообщение для проверки доступа
                async for message in self.client.iter_messages(self.source_entity, limit=1):
                    break
                else:
                    self.logger.warning("Исходная группа/канал пуста или нет доступа к сообщениям")
            except Exception as e:
                self.logger.error(f"Нет доступа к чтению сообщений из исходной группы/канала: {e}")
                return False
            
            # Получаем entity целевой группы/канала
            try:
                self.target_entity = await self.client.get_entity(self.target_group_id)
                self.logger.info(f"Целевая группа/канал найдена: {self.target_entity.title}")
            except Exception as e:
                # Пробуем преобразовать ID канала в число, если это необходимо
                if self.target_group_id.startswith('-100'):
                    try:
                        numeric_id = int(self.target_group_id)
                        self.logger.info(f"Пробуем числовой ID для целевого канала: {numeric_id}")
                        self.target_entity = await self.client.get_entity(numeric_id)
                        self.logger.info(f"Целевой канал найден через числовой ID: {self.target_entity.title}")
                    except Exception as e2:
                        self.logger.error(f"Не удалось получить целевой канал через числовой ID: {e2}")
                        raise e
                else:
                    raise e
            
            # Проверяем права на отправку сообщений в целевую группу/канал
            try:
                # Получаем информацию о правах пользователя
                me = await self.client.get_me()
                
                # Для каналов проверяем права администратора
                if hasattr(self.target_entity, 'broadcast'):
                    # Это канал, проверяем права администратора
                    try:
                        # Используем более простой способ проверки прав
                        full_chat = await self.client(functions.channels.GetFullChannelRequest(self.target_entity))
                        if hasattr(full_chat, 'full_chat') and hasattr(full_chat.full_chat, 'participants_count'):
                            self.logger.info(f"Канал найден, участников: {full_chat.full_chat.participants_count}")
                        
                        # ИСПРАВЛЕННАЯ проверка прав администратора
                        try:
                            permissions = await self.client.get_permissions(self.target_entity, me.id)
                            
                            if permissions.is_admin:
                                self.logger.info("✅ Вы являетесь администратором/создателем целевого канала")
                            else:
                                self.logger.warning("⚠️ Вы не являетесь администратором целевого канала, но продолжаем...")
                                
                        except Exception as e2:
                            self.logger.warning(f"Не удалось проверить права администратора: {e2}")
                            self.logger.info("ℹ️ Продолжаем без проверки прав администратора")
                    except Exception as e:
                        self.logger.warning(f"Не удалось получить информацию о канале: {e}")
                        self.logger.info("ℹ️ Продолжаем без проверки прав")
                else:
                    # Это группа, проверяем участников
                    try:
                        participants = await self.client.get_participants(self.target_entity, limit=1)
                        
                        # Проверяем, является ли пользователь участником целевой группы
                        is_member = False
                        async for participant in self.client.iter_participants(self.target_entity, limit=None):
                            if participant.id == me.id:
                                is_member = True
                                # Проверяем права на отправку сообщений
                                if hasattr(participant, 'participant') and hasattr(participant.participant, 'admin_rights'):
                                    admin_rights = participant.participant.admin_rights
                                    if admin_rights and not admin_rights.post_messages:
                                        self.logger.warning("Ограниченные права администратора в целевой группе")
                                break
                        
                        if not is_member:
                            self.logger.error("Пользователь не является участником целевой группы")
                            return False
                    except Exception as e:
                        self.logger.warning(f"Не удалось проверить права в целевой группе: {e}")
                        # Продолжаем, так как это может быть ограничение API
            except Exception as e:
                self.logger.warning(f"Не удалось проверить права в целевой группе/канале: {e}")
                # Продолжаем, так как это может быть ограничение API
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка инициализации групп/каналов: {e}")
            return False
    
    async def get_total_messages_count(self) -> int:
        """
        Получение общего количества сообщений в исходной группе/канале.
        ИСПРАВЛЕНО: Теперь получает реальное количество всех сообщений.
        
        Returns:
            Общее количество сообщений
        """
        try:
            # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Получаем реальное количество всех сообщений
            # Используем более эффективный метод с GetHistoryRequest
            
            # Получаем первое сообщение для определения максимального ID
            first_message = None
            async for msg in self.client.iter_messages(self.source_entity, limit=1):
                first_message = msg
                break
            
            if not first_message:
                return 0
            
            # Получаем информацию об истории с максимальным ID
            history = await self.client(GetHistoryRequest(
                peer=self.source_entity,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0
            ))
            
            # Получаем реальное количество сообщений
            total_count = getattr(history, 'count', 0)
            
            if total_count > 0:
                self.logger.info(f"Найдено {total_count} сообщений в источнике")
                return total_count
            else:
                # Fallback: подсчет через итерацию (может быть медленным)
                self.logger.warning("Используем fallback подсчет сообщений")
                message_count = 0
                async for message in self.client.iter_messages(self.source_entity, limit=None):
                    message_count += 1
                    # Логируем прогресс каждые 1000 сообщений
                    if message_count % 1000 == 0:
                        self.logger.info(f"Подсчитано {message_count} сообщений...")
                
                return message_count
            
        except Exception as e:
            self.logger.error(f"Ошибка получения количества сообщений: {e}")
            # В случае ошибки возвращаем приблизительную оценку
            return 10000  # Достаточно большое число для прогресс-бара
    
    async def get_comments_for_message(self, message: Message) -> List[Message]:
        """
        Получает комментарии для сообщения из канала через discussion group.
        
        Args:
            message: Сообщение из канала, для которого нужно получить комментарии
            
        Returns:
            Список сообщений-комментариев
        """
        comments = []
        
        try:
            # Проверяем, есть ли у сообщения информация о комментариях
            if not hasattr(message, 'replies') or not message.replies:
                self.logger.debug(f"Сообщение {message.id}: нет атрибута replies")
                return comments
            
            self.logger.debug(f"Сообщение {message.id}: replies = {message.replies}")
            
            # Проверяем, включены ли комментарии
            if not hasattr(message.replies, 'comments') or not message.replies.comments:
                self.logger.debug(f"Сообщение {message.id}: комментарии отключены")
                return comments
                
            # Проверяем, есть ли связанная группа
            if not hasattr(message.replies, 'channel_id') or not message.replies.channel_id:
                self.logger.debug(f"Сообщение {message.id}: нет channel_id в replies")
                return comments
                
            # Получаем discussion group
            discussion_group_id = message.replies.channel_id
            self.logger.info(f"📝 Сообщение {message.id}: найдена discussion group с ID {discussion_group_id}")
            
            try:
                discussion_group = PeerChannel(discussion_group_id)
                
                # Получаем комментарии из discussion group с ограничением по количеству
                comment_count = 0
                max_comments_per_message = 100  # Ограничиваем количество комментариев на сообщение
                
                async for comment in self.client.iter_messages(
                    discussion_group, 
                    reply_to=message.id,
                    limit=max_comments_per_message
                ):
                    comments.append(comment)
                    comment_count += 1
                    
                if comment_count > 0:
                    self.logger.info(f"💬 Сообщение {message.id}: собрано {comment_count} комментариев из discussion group {discussion_group_id}")
                else:
                    self.logger.debug(f"Сообщение {message.id}: комментариев в discussion group не найдено")
                    
            except Exception as group_error:
                self.logger.warning(f"Ошибка доступа к discussion group {discussion_group_id} для сообщения {message.id}: {group_error}")
                
        except Exception as e:
            self.logger.warning(f"Ошибка получения комментариев для сообщения {message.id}: {e}")
            
        return comments
    
    async def copy_all_messages(self, resume_from_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Копирование всех сообщений из исходной группы/канала в целевую группу/канал.
        НОВАЯ ВЕРСИЯ: С поддержкой строгой хронологии и вложенности (комментарии).
        
        Args:
            resume_from_id: ID сообщения для возобновления с определенного места
        
        Returns:
            Словарь со статистикой копирования
        """
        if not await self.initialize():
            return {'error': 'Не удалось инициализировать группы/каналы'}
        
        # НОВОЕ: Инициализируем трекер с информацией о каналах
        if self.message_tracker:
            self.message_tracker.set_channels(
                str(self.source_group_id), 
                str(self.target_group_id)
            )
        
        # Получаем общее количество сообщений
        total_messages = await self.get_total_messages_count()
        if total_messages == 0:
            self.logger.warning("В исходной группе/канале нет сообщений")
            return {'total_messages': 0, 'copied_messages': 0}
        
        self.logger.info(f"Начинаем копирование {total_messages} сообщений")
        progress_tracker = ProgressTracker(total_messages)
        
        # НОВОЕ: Логируем режим обработки комментариев
        if self.flatten_structure:
            self.logger.info("🔄 Режим антивложенности: комментарии будут обработаны как обычные посты")
        else:
            self.logger.info("🔗 Режим с вложенностью: комментарии сохранят связь с основными постами")
        
        # Определяем начальную позицию
        if self.message_tracker and not resume_from_id:
            # Используем трекер для определения последнего ID
            last_copied_id = self.message_tracker.get_last_copied_id()
            if last_copied_id:
                resume_from_id = last_copied_id
                self.logger.info(f"📊 Трекер: последний скопированный ID {last_copied_id}")
        
        min_id = resume_from_id if resume_from_id else 0
        
        try:
            # НОВАЯ АРХИТЕКТУРА: Правильная группировка альбомов
            # Сначала собираем ВСЕ сообщения, затем группируем по альбомам
            self.logger.info("🔄 Начинаем сбор сообщений для правильной группировки альбомов")
            
            # Определяем параметры для iter_messages
            iter_params = {
                'entity': self.source_entity,
                'reverse': True,  # От старых к новым - ключевой параметр для хронологии
                'limit': None     # Все сообщения
                # Примечание: параметр 'replies' не поддерживается в current Telethon API
                # Комментарии обрабатываются как часть общего потока сообщений
            }
            
            # Если возобновляем работу, проверяем наличие новых сообщений
            if min_id:
                self.logger.info(f"Возобновление работы с сообщения ID: {min_id}")
                
                # Проверяем, есть ли новые сообщения после min_id
                has_new_messages = False
                async for test_message in self.client.iter_messages(self.source_entity, min_id=min_id, limit=1):
                    has_new_messages = True
                    break
                
                if not has_new_messages:
                    self.logger.info(f"🎯 Новых сообщений после ID {min_id} не найдено. Копирование актуально.")
                    return {
                        'total_messages': total_messages,
                        'copied_messages': 0,
                        'failed_messages': 0,
                        'skipped_messages': 0,
                        'status': 'up_to_date',
                        'message': f'Все сообщения до ID {min_id} уже скопированы'
                    }
                
                iter_params['min_id'] = min_id  # Исключает сообщения с ID <= min_id
            
            # ЭТАП 1: Собираем все сообщения для правильной группировки
            all_messages = []
            message_count = 0
            
            async for message in self.client.iter_messages(**iter_params):
                message_count += 1
                
                # Проверка дедупликации
                if self.deduplicator.is_message_processed(message):
                    self.logger.debug(f"Сообщение {message.id} уже было обработано ранее, пропускаем")
                    self.skipped_messages += 1
                    continue
                
                all_messages.append(message)
                
                # Логируем прогресс каждые 1000 сообщений
                if len(all_messages) % 1000 == 0:
                    self.logger.info(f"Собрано {len(all_messages)} сообщений для обработки...")
            
            self.logger.info(f"Всего собрано {len(all_messages)} основных сообщений")
            
            # ЭТАП 1.5: Собираем комментарии для каждого сообщения (если включен режим антивложенности)
            if self.flatten_structure:
                self.logger.info("🔄 Сбор комментариев из discussion groups...")
                comments_collected = 0
                messages_processed = 0
                messages_with_comments = 0
                
                # Ограничиваем количество сообщений для проверки комментариев (для предотвращения зависания)
                max_messages_to_check = 1000
                messages_to_check = all_messages[:max_messages_to_check] if len(all_messages) > max_messages_to_check else all_messages[:]
                
                self.logger.info(f"📊 Проверяем комментарии для {len(messages_to_check)} сообщений из {len(all_messages)}")
                
                for message in messages_to_check:
                    messages_processed += 1
                    
                    # Быстрая предварительная проверка наличия replies
                    if not hasattr(message, 'replies') or not message.replies:
                        continue
                        
                    if not hasattr(message.replies, 'comments') or not message.replies.comments:
                        continue
                        
                    # Логируем прогресс каждые 100 сообщений
                    if messages_processed % 100 == 0:
                        self.logger.info(f"🔍 Проверено сообщений: {messages_processed}/{len(messages_to_check)}, найдено комментариев: {comments_collected}")
                    
                    comments = await self.get_comments_for_message(message)
                    if comments:
                        messages_with_comments += 1
                        
                        # Помечаем комментарии специальным атрибутом для последующей идентификации
                        for comment in comments:
                            comment._is_from_discussion_group = True
                            comment._parent_message_id = message.id
                        
                        # Добавляем комментарии к общему списку сообщений
                        all_messages.extend(comments)
                        comments_collected += len(comments)
                
                self.logger.info(f"📊 Результаты сбора комментариев:")
                self.logger.info(f"   🔍 Проверено сообщений: {messages_processed}")
                self.logger.info(f"   📝 Сообщений с комментариями: {messages_with_comments}")
                self.logger.info(f"   💬 Всего собрано комментариев: {comments_collected}")
                
                if comments_collected > 0:
                    self.logger.info(f"✅ Успешно собрано {comments_collected} комментариев из discussion groups")
                else:
                    self.logger.info("ℹ️  Комментарии не найдены или канал не имеет discussion group")
            
            # ЭТАП 1.6: Сортируем все сообщения (основные + комментарии) по хронологии
            if comments_collected > 0:
                self.logger.info("🔄 Сортировка сообщений и комментариев по хронологии...")
                
                # Сортируем все сообщения по дате создания для сохранения хронологии
                all_messages.sort(key=lambda msg: msg.date if hasattr(msg, 'date') and msg.date else msg.id)
                
                self.logger.info(f"✅ Сообщения отсортированы по хронологии")
            
            self.logger.info(f"Всего сообщений (включая комментарии): {len(all_messages)}, начинаем группировку")
            
            # ЭТАП 2: Группируем сообщения по альбомам, НО сохраняем исходный порядок
            grouped_messages = {}  # grouped_id -> список сообщений
            processed_albums = set()  # уже обработанные альбомы
            
            # НОВОЕ: Подсчитываем статистику типов сообщений
            main_posts_count = 0
            comments_count = 0
            albums_in_comments_count = 0
            albums_in_main_count = 0
            
            for message in all_messages:
                # Определяем тип сообщения
                # Комментарии могут быть либо обычными reply, либо из discussion group
                is_comment = (hasattr(message, 'reply_to') and message.reply_to is not None) or \
                           (hasattr(message, '_is_from_discussion_group') and message._is_from_discussion_group)
                
                if is_comment:
                    comments_count += 1
                else:
                    main_posts_count += 1
                
                # Проверяем, является ли сообщение частью альбома
                if hasattr(message, 'grouped_id') and message.grouped_id:
                    if message.grouped_id not in grouped_messages:
                        grouped_messages[message.grouped_id] = []
                        # Подсчитываем новые альбомы по типу
                        if is_comment:
                            albums_in_comments_count += 1
                        else:
                            albums_in_main_count += 1
                    grouped_messages[message.grouped_id].append(message)
                    self.logger.debug(f"Добавлено сообщение {message.id} в альбом {message.grouped_id}")
            
            self.logger.info(f"📊 Статистика сообщений:")
            self.logger.info(f"   📌 Основных постов: {main_posts_count}")
            self.logger.info(f"   💬 Комментариев: {comments_count}")
            self.logger.info(f"   🎬 Альбомов в основных постах: {albums_in_main_count}")
            self.logger.info(f"   🎬 Альбомов в комментариях: {albums_in_comments_count}")
            self.logger.info(f"   📦 Всего альбомов: {len(grouped_messages)}")
            
            # Инициализируем трекер прогресса
            progress_tracker = ProgressTracker(total_messages)
            
            # ЭТАП 3: Обрабатываем сообщения в ИСХОДНОМ ПОРЯДКЕ
            for message in all_messages:
                try:
                    # НОВОЕ: Определяем тип сообщения (основное или комментарий)
                    # Комментарии могут быть либо обычными reply, либо из discussion group
                    is_comment = (hasattr(message, 'reply_to') and message.reply_to is not None) or \
                               (hasattr(message, '_is_from_discussion_group') and message._is_from_discussion_group)
                    
                    # Проверяем, является ли сообщение частью альбома
                    if hasattr(message, 'grouped_id') and message.grouped_id:
                        grouped_id = message.grouped_id
                        
                        # Если этот альбом уже обработан, пропускаем
                        if grouped_id in processed_albums:
                            continue
                        
                        # Обрабатываем весь альбом целиком
                        album_messages = grouped_messages[grouped_id]
                        album_messages.sort(key=lambda x: x.id)  # Сортируем по ID для правильного порядка
                        
                        # Логируем тип альбома
                        album_type = "в комментарии" if is_comment else "основной"
                        self.logger.info(f"🎬 Обрабатываем альбом {grouped_id} ({album_type}) из {len(album_messages)} сообщений")
                        
                        # Вычисляем общий размер альбома для мониторинга
                        total_size = 0
                        for msg in album_messages:
                            if msg.media and hasattr(msg.media, 'document') and msg.media.document:
                                total_size += getattr(msg.media.document, 'size', 0)
                            elif msg.message:
                                total_size += len(msg.message.encode('utf-8'))
                        
                        # Копируем альбом как единое целое
                        # В режиме flatten_structure комментарии обрабатываются как обычные посты
                        success = await self.copy_album(album_messages)
                        
                        # Обновляем статистику для всех сообщений альбома
                        for msg in album_messages:
                            progress_tracker.update(success)
                            self.performance_monitor.record_message_processed(success, total_size // len(album_messages))
                        
                        if success:
                            self.copied_messages += len(album_messages)
                            album_status = "✅ успешно скопирован" if not is_comment else "✅ успешно скопирован (комментарий)"
                            self.logger.info(f"{album_status}: альбом {grouped_id}")
                            
                            # Записываем ID последнего сообщения альбома
                            last_album_message_id = max(msg.id for msg in album_messages)
                            save_last_message_id(last_album_message_id, self.resume_file)
                            self.logger.debug(f"Записан ID {last_album_message_id} после успешного копирования альбома")
                        else:
                            self.failed_messages += len(album_messages)
                            album_status = "❌ не удалось скопировать" if not is_comment else "❌ не удалось скопировать (комментарий)"
                            self.logger.warning(f"{album_status}: альбом {grouped_id}")
                        
                        # Помечаем альбом как обработанный
                        processed_albums.add(grouped_id)
                        
                        # Соблюдаем лимиты скорости
                        if not self.dry_run:
                            await self.rate_limiter.wait_if_needed()
                            if success:
                                self.rate_limiter.record_message_sent()
                    
                    else:
                        # Обычное одиночное сообщение (основное или комментарий)
                        # Вычисляем размер сообщения для мониторинга
                        message_size = 0
                        if message.media and hasattr(message.media, 'document') and message.media.document:
                            message_size = getattr(message.media.document, 'size', 0)
                        elif message.message:
                            message_size = len(message.message.encode('utf-8'))
                        
                        # Логируем тип сообщения
                        message_type = "💬 комментарий" if is_comment else "📌 пост"
                        if not self.flatten_structure and is_comment:
                            # В режиме с вложенностью можно добавить специальную обработку
                            self.logger.debug(f"Обрабатываем {message_type} {message.id} (связан с {message.reply_to})")
                        else:
                            self.logger.debug(f"Обрабатываем {message_type} {message.id}")
                        
                        # Копируем сообщение
                        success = await self.copy_single_message(message)
                        progress_tracker.update(success)
                        
                        # Записываем в мониторинг производительности
                        self.performance_monitor.record_message_processed(success, message_size)
                        
                        if success:
                            self.copied_messages += 1
                            success_status = "✅ успешно скопировано" if not is_comment else "✅ успешно скопировано (комментарий)"
                            self.logger.debug(f"{success_status}: сообщение {message.id}")
                            save_last_message_id(message.id, self.resume_file)
                            self.logger.debug(f"Записан ID {message.id} после успешного копирования")
                        else:
                            self.failed_messages += 1
                            fail_status = "❌ не удалось скопировать" if not is_comment else "❌ не удалось скопировать (комментарий)"
                            self.logger.warning(f"{fail_status}: сообщение {message.id}")
                        
                        # Соблюдаем лимиты скорости
                        if not self.dry_run:
                            await self.rate_limiter.wait_if_needed()
                            if success:
                                self.rate_limiter.record_message_sent()
                
                except FloodWaitError as e:
                    await handle_flood_wait(e, self.logger)
                    # Повторяем попытку для текущего сообщения
                    if hasattr(message, 'grouped_id') and message.grouped_id and message.grouped_id not in processed_albums:
                        # Повторяем альбом
                        grouped_id = message.grouped_id
                        album_messages = grouped_messages[grouped_id]
                        album_messages.sort(key=lambda x: x.id)
                        success = await self.copy_album(album_messages)
                        
                        for msg in album_messages:
                            progress_tracker.update(success)
                        
                        if success:
                            self.copied_messages += len(album_messages)
                            self.logger.info(f"✅ Альбом {grouped_id} успешно скопирован после FloodWait")
                            last_album_message_id = max(msg.id for msg in album_messages)
                            save_last_message_id(last_album_message_id, self.resume_file)
                            self.logger.debug(f"Записан ID {last_album_message_id} после успешного копирования альбома (FloodWait)")
                            if not self.dry_run:
                                self.rate_limiter.record_message_sent()
                        else:
                            self.failed_messages += len(album_messages)
                            self.logger.warning(f"❌ Не удалось скопировать альбом {grouped_id} даже после FloodWait")
                        
                        processed_albums.add(grouped_id)
                        
                    else:
                        # Повторяем одиночное сообщение
                        success = await self.copy_single_message(message)
                        progress_tracker.update(success)
                        
                        if success:
                            self.copied_messages += 1
                            self.logger.debug(f"✅ Сообщение {message.id} успешно скопировано после FloodWait")
                            save_last_message_id(message.id, self.resume_file)
                            self.logger.debug(f"Записан ID {message.id} после успешного копирования (FloodWait)")
                            if not self.dry_run:
                                self.rate_limiter.record_message_sent()
                        else:
                            self.failed_messages += 1
                            self.logger.warning(f"❌ Не удалось скопировать сообщение {message.id} даже после FloodWait")
                
                except (PeerFloodError, MediaInvalidError) as e:
                    if hasattr(message, 'grouped_id') and message.grouped_id:
                        grouped_id = message.grouped_id
                        if grouped_id not in processed_albums:
                            album_messages = grouped_messages[grouped_id]
                            self.logger.warning(f"Telegram API ошибка для альбома {grouped_id}: {e}")
                            self.failed_messages += len(album_messages)
                            for msg in album_messages:
                                progress_tracker.update(False)
                            processed_albums.add(grouped_id)
                    else:
                        self.logger.warning(f"Telegram API ошибка для сообщения {message.id}: {e}")
                        self.failed_messages += 1
                        progress_tracker.update(False)
                
                except Exception as e:
                    if hasattr(message, 'grouped_id') and message.grouped_id:
                        grouped_id = message.grouped_id
                        if grouped_id not in processed_albums:
                            album_messages = grouped_messages[grouped_id]
                            self.logger.error(f"Неожиданная ошибка копирования альбома {grouped_id}: {type(e).__name__}: {e}")
                            self.failed_messages += len(album_messages)
                            for msg in album_messages:
                                progress_tracker.update(False)
                            processed_albums.add(grouped_id)
                    else:
                        self.logger.error(f"Неожиданная ошибка копирования сообщения {message.id}: {type(e).__name__}: {e}")
                        self.failed_messages += 1
                        progress_tracker.update(False)
            
            self.logger.info(f"✅ Обработано {len(all_messages)} сообщений в исходном порядке")
        
        except Exception as e:
            self.logger.error(f"Критическая ошибка при копировании: {e}")
            return {'error': str(e)}
        
        # Получаем финальную статистику
        final_stats = progress_tracker.get_final_stats()
        final_stats.update({
            'copied_messages': self.copied_messages,
            'failed_messages': self.failed_messages,
            'skipped_messages': self.skipped_messages
        })
        
        # НОВОЕ: Проверяем реальное количество сообщений в целевом канале
        if self.copied_messages > 0 and not self.dry_run:
            try:
                target_count = await self.get_target_messages_count()
                final_stats['target_messages_count'] = target_count
                self.logger.info(f"🔍 Проверка: в целевом канале {target_count} сообщений")
            except Exception as e:
                self.logger.warning(f"Не удалось проверить целевой канал: {e}")
        
        self.logger.info(f"📊 Копирование завершено. Скопировано: {self.copied_messages}, "
                        f"Ошибок: {self.failed_messages}, Пропущено: {self.skipped_messages}")
        
        return final_stats
    

    

    

    
    async def get_target_messages_count(self) -> int:
        """
        Получение количества сообщений в целевом канале для проверки.
        
        Returns:
            Количество сообщений в целевом канале
        """
        try:
            # Получаем информацию об истории целевого канала
            history = await self.client(GetHistoryRequest(
                peer=self.target_entity,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0
            ))
            
            target_count = getattr(history, 'count', 0)
            self.logger.info(f"📊 В целевом канале найдено {target_count} сообщений")
            return target_count
            
        except Exception as e:
            self.logger.warning(f"Не удалось получить количество сообщений в целевом канале: {e}")
            return 0
    
    async def copy_album(self, album_messages: List[Message]) -> bool:
        """
        Копирование альбома сообщений как единого целого.
        ИСПРАВЛЕНО: Теперь работает с правильно собранными альбомами.
        
        Args:
            album_messages: Список сообщений альбома (отсортированный по ID)
        
        Returns:
            True если копирование успешно, False иначе
        """
        try:
            if not album_messages:
                return False
            
            # Получаем первое сообщение для текста и форматирования
            first_message = album_messages[0]
            
            self.logger.debug(f"Копируем альбом из {len(album_messages)} сообщений")
            
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Альбом из {len(album_messages)} сообщений: {first_message.message[:50] if first_message.message else 'медиа'}")
                return True
            
            # Собираем все медиа файлы из альбома
            media_files = []
            for message in album_messages:
                if message.media:
                    media_files.append(message.media)
            
            if not media_files:
                self.logger.warning("Альбом не содержит медиа файлов")
                # Если нет медиа, отправляем как обычное текстовое сообщение
                if first_message.message:
                    return await self.copy_single_message(first_message)
                return False
            
            # Получаем текст из первого сообщения альбома
            caption = first_message.message or ""
            
            # Подготавливаем параметры для отправки альбома
            send_kwargs = {
                'entity': self.target_entity,
                'file': media_files,  # Массив медиа файлов
                'caption': caption,
            }
            
            # ВАЖНО: Сохраняем форматирование текста из первого сообщения
            if first_message.entities:
                send_kwargs['formatting_entities'] = first_message.entities
            
            self.logger.debug(f"Отправляем альбом из {len(media_files)} медиа файлов")
            
            # Отправляем альбом как группированные медиа
            sent_messages = await self.client.send_file(**send_kwargs)
            
            # Анализируем результат
            if isinstance(sent_messages, list):
                self.logger.info(f"✅ Альбом успешно отправлен как {len(sent_messages)} сообщений")
                
                # Обновляем трекер с реальными ID отправленных сообщений
                if self.message_tracker and sent_messages:
                    source_ids = [msg.id for msg in album_messages]
                    target_ids = [msg.id for msg in sent_messages]
                    self.message_tracker.mark_album_copied(source_ids, target_ids)
            else:
                self.logger.warning(f"⚠️ Альбом отправлен как одно сообщение {sent_messages.id} - возможна потеря группировки")
                
                # Если получили одно сообщение вместо альбома
                if self.message_tracker:
                    source_ids = [msg.id for msg in album_messages]
                    target_ids = [sent_messages.id]
                    self.message_tracker.mark_album_copied(source_ids, target_ids)
            
            return True
            
        except MediaInvalidError as e:
            self.logger.warning(f"Медиа альбома недоступно: {e}")
            # Пытаемся отправить только текст из первого сообщения
            if album_messages and album_messages[0].message:
                try:
                    first_message = album_messages[0]
                    text_kwargs = {
                        'entity': self.target_entity,
                        'message': first_message.message,
                        'link_preview': False
                    }
                    if first_message.entities:
                        text_kwargs['formatting_entities'] = first_message.entities
                    await self.client.send_message(**text_kwargs)
                    self.logger.info(f"Отправлен только текст альбома (медиа недоступно)")
                    return True
                except Exception as text_error:
                    self.logger.error(f"Ошибка отправки текста альбома: {text_error}")
            return False
            
        except Exception as e:
            self.logger.error(f"Ошибка копирования альбома: {e}")
            return False
    
    async def copy_single_message(self, message: Message) -> bool:
        """
        Копирование одного сообщения.
        
        Args:
            message: Сообщение для копирования
        
        Returns:
            True если копирование успешно, False иначе
        """
        try:
            # Пропускаем служебные сообщения
            if not message.message and not message.media:
                self.skipped_messages += 1
                return True
            
            # ВАЖНО: Этот метод теперь используется только для одиночных сообщений
            # Альбомы обрабатываются отдельно в copy_album()
            if hasattr(message, 'grouped_id') and message.grouped_id:
                self.logger.warning(f"Сообщение {message.id} является частью альбома {message.grouped_id}, но обрабатывается как одиночное")
            
            self.logger.debug(f"Копируем одиночное сообщение {message.id}")
            
            if self.dry_run:
                self.logger.info(f"[DRY RUN] Сообщение {message.id}: {message.message[:50] if message.message else 'медиа'}")
                return True
            
            # ИСПРАВЛЕННОЕ 1:1 копирование с сохранением всего форматирования
            text = message.message or ""
            
            # Подготавливаем параметры для отправки
            send_kwargs = {
                'entity': self.target_entity,
                'message': text,
                'link_preview': False  # Отключаем предварительный просмотр ссылок для точного копирования
            }
            
            # ИСПРАВЛЕНИЕ: Правильное сохранение форматирования
            # Если есть entities, используем их напрямую, иначе parse_mode=None
            if message.entities:
                send_kwargs['formatting_entities'] = message.entities
            
            # Обрабатываем медиа для полного 1:1 копирования
            if message.media:
                if isinstance(message.media, MessageMediaPhoto):
                    # Для фотографий используем send_file с оригинальным медиа
                    file_kwargs = {
                        'entity': self.target_entity,
                        'file': message.media,
                        'caption': text,
                        'force_document': False
                    }
                    if message.entities:
                        file_kwargs['formatting_entities'] = message.entities
                    sent_message = await self.client.send_file(**file_kwargs)
                    
                elif isinstance(message.media, MessageMediaDocument):
                    # Для документов/видео/аудио используем send_file
                    file_kwargs = {
                        'entity': self.target_entity,
                        'file': message.media,
                        'caption': text,
                        'force_document': True
                    }
                    if message.entities:
                        file_kwargs['formatting_entities'] = message.entities
                    sent_message = await self.client.send_file(**file_kwargs)
                elif isinstance(message.media, MessageMediaWebPage):
                    # Для веб-страниц отправляем только текст с entities
                    sent_message = await self.client.send_message(**send_kwargs)
                else:
                    # Для других типов медиа пытаемся отправить как есть
                    try:
                        file_kwargs = {
                            'entity': self.target_entity,
                            'file': message.media,
                            'caption': text
                        }
                        if message.entities:
                            file_kwargs['formatting_entities'] = message.entities
                        sent_message = await self.client.send_file(**file_kwargs)
                    except Exception as media_error:
                        self.logger.warning(f"Не удалось отправить медиа {type(message.media)}: {media_error}")
                        # Отправляем только текст
                        sent_message = await self.client.send_message(**send_kwargs)
            else:
                # Отправляем текстовое сообщение с сохранением форматирования
                sent_message = await self.client.send_message(**send_kwargs)
            
            # ИСПРАВЛЕНИЕ: Обновляем трекер с реальным ID отправленного сообщения
            if self.message_tracker and sent_message:
                self.message_tracker.mark_message_copied(message.id, sent_message.id)
            
            self.logger.debug(f"Сообщение {message.id} успешно скопировано как {sent_message.id}")
            return True
            
        except MediaInvalidError as e:
            self.logger.warning(f"Медиа сообщения {message.id} недоступно: {e}")
            # Пытаемся отправить только текст с сохранением форматирования
            if message.message:
                try:
                    text_kwargs = {
                        'entity': self.target_entity,
                        'message': message.message,
                        'link_preview': False
                    }
                    if message.entities:
                        text_kwargs['formatting_entities'] = message.entities
                    await self.client.send_message(**text_kwargs)
                    self.logger.info(f"Отправлен только текст сообщения {message.id} (медиа недоступно)")
                    return True
                except Exception as text_error:
                    self.logger.error(f"Ошибка отправки текста сообщения {message.id}: {text_error}")
            return False
            
        except Exception as e:
            self.logger.error(f"Ошибка копирования сообщения {message.id}: {e}")
            return False
    
    def cleanup_temp_files(self) -> None:
        """Очистка временных файлов."""
        temp_dir = "temp_media"
        if os.path.exists(temp_dir):
            try:
                for file_name in os.listdir(temp_dir):
                    file_path = os.path.join(temp_dir, file_name)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                os.rmdir(temp_dir)
                self.logger.info("Временные файлы очищены")
            except Exception as e:
                self.logger.warning(f"Ошибка очистки временных файлов: {e}")
    
    async def copy_messages_range(self, start_id: int, end_id: int) -> Dict[str, Any]:
        """
        Копирование сообщений в определенном диапазоне ID.
        
        Args:
            start_id: Начальный ID сообщения
            end_id: Конечный ID сообщения
        
        Returns:
            Статистика копирования
        """
        if not await self.initialize():
            return {'error': 'Не удалось инициализировать группы'}
        
        self.logger.info(f"Копирование сообщений с {start_id} по {end_id}")
        
        copied = 0
        failed = 0
        
        try:
            async for message in self.client.iter_messages(
                self.source_entity,
                min_id=start_id - 1,
                max_id=end_id + 1,
                reverse=True
            ):
                if start_id <= message.id <= end_id:
                    success = await self.copy_single_message(message)
                    if success:
                        copied += 1
                    else:
                        failed += 1
                    
                    if not self.dry_run:
                        await self.rate_limiter.wait_if_needed()
        
        except Exception as e:
            self.logger.error(f"Ошибка копирования диапазона: {e}")
            return {'error': str(e)}
        
        return {
            'copied_messages': copied,
            'failed_messages': failed,
            'start_id': start_id,
            'end_id': end_id
        }