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
    ChannelParticipantAdmin, ChannelParticipantCreator, PeerChannel,
    DocumentAttributeFilename
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
                 add_debug_tags: bool = False, flatten_structure: bool = False, 
                 debug_message_ids: bool = False, batch_size: int = 100):
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
            debug_message_ids: Добавлять ли ID сообщений к тексту для отладки
            batch_size: Размер батча для обработки сообщений (по умолчанию 100)
        """
        self.client = client
        self.source_group_id = source_group_id
        self.target_group_id = target_group_id
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        self.resume_file = resume_file
        self.logger = logging.getLogger('telegram_copier.copier')
        
        # ИСПРАВЛЕНИЕ: Размер батча для предотвращения проблем с памятью
        self.batch_size = batch_size
        self.logger.info(f"🔧 Инициализирован копировщик с размером батча: {batch_size}")
        
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
        
        # НОВОЕ: Отладочный режим - добавление ID к сообщениям
        self.debug_message_ids = debug_message_ids
        if self.debug_message_ids:
            self.logger.info("🐛 Включен режим отладки - ID сообщений будут добавлены к тексту")
        
        # НОВОЕ: Режим антивложенности
        self.flatten_structure = flatten_structure
        if self.flatten_structure:
            self.logger.info("🔄 Включен режим антивложенности - комментарии будут превращены в обычные посты")
        
        # НОВОЕ: Кэш комментариев для батчевой обработки
        self.comments_cache = {}  # {channel_post_id: [comments]}
        self.comments_cache_loaded = False
        self.discussion_groups_cache = set()  # Кэш ID discussion groups
        
        # Инициализация трекера сообщений
        if self.use_message_tracker:
            self.message_tracker = MessageTracker(tracker_file)
            self.logger.info(f"✅ Включен детальный трекинг сообщений: {tracker_file}")
        else:
            self.message_tracker = None
            self.logger.info("ℹ️ Используется простой трекинг (last_message_id.txt)")
        
        # Очистка старых хешей при инициализации
        self.deduplicator.cleanup_old_hashes()
    
    def _add_debug_id_to_text(self, text: str, message_id: int) -> str:
        """
        Добавляет ID сообщения к тексту в режиме отладки.
        
        Args:
            text: Оригинальный текст сообщения
            message_id: ID сообщения
            
        Returns:
            Текст с добавленным ID (если включен debug режим)
        """
        if not self.debug_message_ids:
            return text
        
        debug_suffix = f"\n\n🐛 DEBUG: Message ID {message_id}"
        
        if text:
            return text + debug_suffix
        else:
            # Если оригинального текста нет, добавляем только debug ID
            return debug_suffix.strip()
    
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
    
    async def get_all_comments_from_discussion_group(self, discussion_group_id: int) -> Dict[int, List[Message]]:
        """
        Получает все комментарии из discussion group и группирует их по ID постов канала.
        
        Args:
            discussion_group_id: ID discussion group
            
        Returns:
            Словарь {channel_post_id: [comments]}
        """
        comments_by_post = {}
        
        try:
            discussion_group = PeerChannel(discussion_group_id)
            self.logger.info(f"🔍 Получаем все сообщения из discussion group {discussion_group_id}")
            
            message_count = 0
            forward_messages = {}  # channel_post_id -> discussion_message_id
            all_comments = []
            
            # Получаем все сообщения из discussion group
            async for disc_message in self.client.iter_messages(discussion_group, limit=None):
                message_count += 1
                
                if message_count % 1000 == 0:
                    self.logger.info(f"   📥 Обработано {message_count} сообщений из discussion group...")
                
                # Если это пересланное сообщение из канала
                if (hasattr(disc_message, 'forward') and disc_message.forward and 
                    hasattr(disc_message.forward, 'channel_post')):
                    channel_post_id = disc_message.forward.channel_post
                    forward_messages[channel_post_id] = disc_message.id
                    self.logger.debug(f"Найдено пересланное сообщение: канал {channel_post_id} -> discussion {disc_message.id}")
                
                # Если это комментарий (reply_to существует)
                elif hasattr(disc_message, 'reply_to') and disc_message.reply_to:
                    all_comments.append(disc_message)
            
            self.logger.info(f"📊 Обработано {message_count} сообщений, найдено {len(forward_messages)} переслок и {len(all_comments)} комментариев")
            
            # Группируем комментарии по постам канала
            for comment in all_comments:
                reply_to_id = comment.reply_to.reply_to_msg_id
                
                # Находим, к какому посту канала относится этот комментарий
                for channel_post_id, discussion_msg_id in forward_messages.items():
                    if discussion_msg_id == reply_to_id:
                        if channel_post_id not in comments_by_post:
                            comments_by_post[channel_post_id] = []
                        comments_by_post[channel_post_id].append(comment)
                        break
            
            self.logger.info(f"✅ Комментарии сгруппированы для {len(comments_by_post)} постов")
            
        except Exception as e:
            self.logger.error(f"Ошибка получения комментариев из discussion group {discussion_group_id}: {e}")
        
        return comments_by_post
    
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
                
                # Получаем комментарии к конкретному сообщению из discussion group
                comment_count = 0
                
                # В discussion groups сообщения из канала дублируются с собственными ID
                # Нужно найти дублированное сообщение и получить комментарии к нему
                try:
                    # НОВЫЙ ПОДХОД: Используем прямой доступ к сообщению в discussion group
                    # Часто ID сообщения в discussion group совпадает с ID в канале
                    target_discussion_message_id = None
                    
                    # Метод 1: Пробуем прямой доступ по ID
                    try:
                        direct_message = await self.client.get_messages(discussion_group, ids=message.id)
                        if direct_message and not direct_message.empty:
                            target_discussion_message_id = message.id
                            self.logger.debug(f"Найдено сообщение через прямой доступ: канал {message.id} -> discussion {message.id}")
                    except Exception as direct_error:
                        self.logger.debug(f"Прямой доступ не удался для сообщения {message.id}: {direct_error}")
                    
                    # Метод 2: Поиск через forward, если прямой доступ не сработал
                    if not target_discussion_message_id:
                        checked_messages = 0
                        
                        # Ищем сообщение с forward_header, указывающим на наш пост
                        async for disc_message in self.client.iter_messages(
                            discussion_group,
                            limit=200  # Разумный лимит
                        ):
                            checked_messages += 1
                            
                            # Проверяем, есть ли forward_header
                            if (hasattr(disc_message, 'forward') and disc_message.forward and 
                                hasattr(disc_message.forward, 'channel_post')):
                                
                                # Проверяем, что это пересланное сообщение из нашего канала
                                if disc_message.forward.channel_post == message.id:
                                    target_discussion_message_id = disc_message.id
                                    self.logger.debug(f"Найдено через forward: канал {message.id} -> discussion {disc_message.id} (проверено {checked_messages} сообщений)")
                                    break
                        
                        if not target_discussion_message_id:
                            self.logger.debug(f"Forward поиск не дал результатов для сообщения {message.id} после проверки {checked_messages} сообщений")
                    
                    # Метод 3: Поиск по содержимому, если предыдущие методы не сработали
                    if not target_discussion_message_id:
                        message_text = getattr(message, 'message', '').strip()
                        if message_text and len(message_text) > 20:  # Только если есть достаточно текста
                            search_text = message_text[:100]  # Первые 100 символов для более точного поиска
                            self.logger.debug(f"Пробуем поиск по тексту для сообщения {message.id}: '{search_text[:30]}...'")
                            
                            async for disc_message in self.client.iter_messages(
                                discussion_group,
                                limit=50,
                                reverse=True  # Ищем от старых к новым
                            ):
                                if hasattr(disc_message, 'message') and disc_message.message:
                                    disc_text = disc_message.message.strip()
                                    # Проверяем совпадение по тексту
                                    if disc_text and (search_text in disc_text or disc_text in search_text):
                                        target_discussion_message_id = disc_message.id
                                        self.logger.debug(f"Найдено через текстовый поиск: канал {message.id} -> discussion {disc_message.id}")
                                        break
                    
                    if target_discussion_message_id:
                        # Теперь получаем комментарии к найденному сообщению
                        self.logger.debug(f"Ищем комментарии к сообщению {target_discussion_message_id} в discussion group {discussion_group_id}")
                        async for comment in self.client.iter_messages(
                            discussion_group, 
                            reply_to=target_discussion_message_id,
                            limit=None
                        ):
                            comments.append(comment)
                            comment_count += 1
                            
                            # Логируем прогресс для сообщений с большим количеством комментариев
                            if comment_count % 500 == 0:
                                self.logger.debug(f"   📥 Сообщение {message.id}: собрано {comment_count} комментариев...")
                    else:
                        self.logger.warning(f"Сообщение {message.id}: не найдено соответствующее сообщение в discussion group {discussion_group_id}")
                            
                except asyncio.TimeoutError:
                    self.logger.warning(f"Тайм-аут при сборе комментариев для сообщения {message.id} (собрано {comment_count})")
                except Exception as iter_error:
                    self.logger.warning(f"Ошибка при итерации комментариев для сообщения {message.id}: {iter_error}")
                    
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
                messages_with_comments = 0
                
                # Определяем уникальные discussion groups
                discussion_groups = set()
                for message in all_messages:
                    if (hasattr(message, 'replies') and message.replies and
                        hasattr(message.replies, 'comments') and message.replies.comments and
                        hasattr(message.replies, 'channel_id') and message.replies.channel_id):
                        discussion_groups.add(message.replies.channel_id)
                
                if discussion_groups:
                    self.logger.info(f"📊 Найдено {len(discussion_groups)} уникальных discussion groups")
                    
                    # Собираем комментарии из всех discussion groups
                    all_comments_by_post = {}
                    for discussion_group_id in discussion_groups:
                        comments_by_post = await self.get_all_comments_from_discussion_group(discussion_group_id)
                        all_comments_by_post.update(comments_by_post)
                    
                    # Добавляем комментарии к соответствующим сообщениям
                    for message in all_messages[:]:
                        if message.id in all_comments_by_post:
                            comments = all_comments_by_post[message.id]
                            messages_with_comments += 1
                            
                            # Помечаем комментарии специальным атрибутом для последующей идентификации
                            for comment in comments:
                                comment._is_from_discussion_group = True
                                comment._parent_message_id = message.id
                            
                            # Добавляем комментарии к общему списку сообщений
                            all_messages.extend(comments)
                            comments_collected += len(comments)
                            
                            self.logger.info(f"💬 Сообщение {message.id}: добавлено {len(comments)} комментариев")
                    
                    self.logger.info(f"📊 Результаты сбора комментариев:")
                    self.logger.info(f"   📝 Сообщений с комментариями: {messages_with_comments}")
                    self.logger.info(f"   💬 Всего собрано комментариев: {comments_collected}")
                    
                    if comments_collected > 0:
                        self.logger.info(f"✅ Успешно собрано {comments_collected} комментариев из discussion groups")
                    else:
                        self.logger.info("ℹ️  Комментарии не найдены в discussion groups")
                else:
                    self.logger.info("ℹ️  Discussion groups не найдены или канал не имеет комментариев")
            
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
    
    async def copy_all_messages_batch(self, resume_from_id: Optional[int] = None) -> Dict[str, Any]:
        """
        ИСПРАВЛЕННЫЙ метод копирования всех сообщений с батчевой обработкой.
        РЕШАЕТ ПРОБЛЕМУ ПАМЯТИ И ХРОНОЛОГИИ после 500-600 постов.
        
        Args:
            resume_from_id: ID сообщения для возобновления с определенного места
        
        Returns:
            Словарь со статистикой копирования
        """
        if not await self.initialize():
            return {'error': 'Не удалось инициализировать группы/каналы'}
        
        # Инициализируем трекер с информацией о каналах
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
        
        self.logger.info(f"🔄 Начинаем БАТЧЕВОЕ копирование {total_messages} сообщений")
        self.logger.info(f"📦 Размер батча: {self.batch_size} сообщений")
        
        # ИСПРАВЛЕНИЕ: Логируем режим обработки комментариев (как в оригинальном методе)
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
        
        # Инициализируем трекер прогресса
        progress_tracker = ProgressTracker(total_messages)
        
        try:
            # НОВАЯ АРХИТЕКТУРА: Батчевая обработка
            batch_number = 1
            
                    # Получаем батчи сообщений и обрабатываем их по порядку
        async for batch in self._get_message_batches(min_id):
            if not batch:  # Пустой батч - конец
                break
            
            self.logger.info(f"📦 Обрабатываем батч #{batch_number}: {len(batch)} сообщений")
            
            # НОВОЕ: Предварительно загружаем комментарии для батча (только один раз)
            if not self.comments_cache_loaded:
                await self.preload_all_comments_cache(batch)
            
            # Обрабатываем батч в хронологическом порядке
            batch_stats = await self._process_message_batch(batch, progress_tracker)
                
                # Обновляем общую статистику
                self.copied_messages += batch_stats['copied']
                self.failed_messages += batch_stats['failed'] 
                self.skipped_messages += batch_stats['skipped']
                
                # Логируем прогресс батча
                self.logger.info(f"✅ Батч #{batch_number} завершен: "
                               f"скопировано {batch_stats['copied']}, "
                               f"ошибок {batch_stats['failed']}, "
                               f"пропущено {batch_stats['skipped']}")
                
                # Сохраняем прогресс после каждого батча
                if batch:
                    last_message_id = max(msg.id for msg in batch)
                    save_last_message_id(last_message_id, self.resume_file)
                    self.logger.debug(f"💾 Прогресс сохранен: последний ID {last_message_id}")
                
                # Принудительная очистка памяти после каждого батча
                await self._cleanup_batch_memory()
                
                batch_number += 1
                
                # Проверяем, достигли ли мы конца
                if len(batch) < self.batch_size:
                    self.logger.info("📄 Достигнут конец канала (батч меньше размера)")
                    break
        
        except Exception as e:
            self.logger.error(f"Критическая ошибка при батчевом копировании: {e}")
            return {'error': str(e)}
        
        # Получаем финальную статистику
        final_stats = progress_tracker.get_final_stats()
        final_stats.update({
            'copied_messages': self.copied_messages,
            'failed_messages': self.failed_messages,
            'skipped_messages': self.skipped_messages,
            'batches_processed': batch_number - 1,
            'batch_size': self.batch_size
        })
        
        # Проверяем реальное количество сообщений в целевом канале
        if self.copied_messages > 0 and not self.dry_run:
            try:
                target_count = await self.get_target_messages_count()
                final_stats['target_messages_count'] = target_count
                self.logger.info(f"🔍 Проверка: в целевом канале {target_count} сообщений")
            except Exception as e:
                self.logger.warning(f"Не удалось проверить целевой канал: {e}")
        
        self.logger.info(f"🎉 Батчевое копирование завершено! "
                        f"Обработано {batch_number-1} батчей. "
                        f"Скопировано: {self.copied_messages}, "
                        f"Ошибок: {self.failed_messages}, "
                        f"Пропущено: {self.skipped_messages}")
        
        return final_stats
    
    async def _get_message_batches(self, min_id: int = 0):
        """
        Генератор батчей сообщений в хронологическом порядке.
        КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Не загружает все сообщения в память!
        
        Args:
            min_id: Минимальный ID для начала обработки
            
        Yields:
            Батчи сообщений размером self.batch_size
        """
        offset_id = 0  # Начинаем с самых новых для reverse=True
        
        while True:
            try:
                # Параметры для получения батча
                iter_params = {
                    'entity': self.source_entity,
                    'limit': self.batch_size,
                    'reverse': True,  # От старых к новым
                }
                
                # Добавляем offset_id если не первый запрос
                if offset_id > 0:
                    iter_params['offset_id'] = offset_id
                
                # Добавляем min_id если возобновляем работу
                if min_id > 0:
                    iter_params['min_id'] = min_id
                
                # Получаем батч сообщений
                batch = []
                
                async for message in self.client.iter_messages(**iter_params):
                    # Проверка дедупликации
                    if self.deduplicator.is_message_processed(message):
                        self.logger.debug(f"Сообщение {message.id} уже обработано, пропускаем")
                        continue
                    
                    batch.append(message)
                
                # Если батч пустой - конец
                if not batch:
                    break
                
                # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Проверяем, не разрезали ли альбом
                batch = await self._ensure_complete_albums(batch, iter_params)
                
                # Обновляем offset_id для следующего батча
                # В reverse=True режиме берем ID последнего (самого старого) сообщения
                offset_id = batch[-1].id
                
                self.logger.debug(f"📦 Получен батч из {len(batch)} сообщений "
                                f"(ID от {batch[0].id} до {batch[-1].id})")
                
                yield batch
                
                # Если батч меньше размера - это последний батч
                if len(batch) < self.batch_size:
                    break
                    
            except Exception as e:
                self.logger.error(f"Ошибка получения батча сообщений: {e}")
                break
    
    async def _ensure_complete_albums(self, batch: List[Message], iter_params: dict) -> List[Message]:
        """
        КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Убеждаемся, что альбомы не разрезаны между батчами.
        
        Args:
            batch: Исходный батч сообщений
            iter_params: Параметры для iter_messages
            
        Returns:
            Батч с полными альбомами
        """
        if not batch:
            return batch
        
        # Находим альбомы в батче
        albums_in_batch = {}
        for message in batch:
            if hasattr(message, 'grouped_id') and message.grouped_id:
                if message.grouped_id not in albums_in_batch:
                    albums_in_batch[message.grouped_id] = []
                albums_in_batch[message.grouped_id].append(message)
        
        if not albums_in_batch:
            return batch  # Нет альбомов - батч готов
        
        # Проверяем, нет ли неполных альбомов в конце батча
        last_message = batch[-1]
        incomplete_albums = []
        
        for grouped_id, album_messages in albums_in_batch.items():
            # Если последнее сообщение батча принадлежит альбому,
            # проверяем, есть ли еще сообщения этого альбома после батча
            if any(msg.id == last_message.id for msg in album_messages):
                try:
                    # Проверяем следующие сообщения
                    check_params = iter_params.copy()
                    check_params['offset_id'] = last_message.id
                    check_params['limit'] = 10  # Проверяем следующие 10 сообщений
                    
                    async for next_message in self.client.iter_messages(**check_params):
                        if (hasattr(next_message, 'grouped_id') and 
                            next_message.grouped_id == grouped_id):
                            # Найдено продолжение альбома - добавляем к батчу
                            batch.append(next_message)
                            album_messages.append(next_message)
                            self.logger.debug(f"Добавлено сообщение {next_message.id} для завершения альбома {grouped_id}")
                        else:
                            # Альбом завершен
                            break
                            
                except Exception as e:
                    self.logger.warning(f"Ошибка проверки альбома {grouped_id}: {e}")
        
        # Сортируем батч по ID для сохранения хронологии
        batch.sort(key=lambda msg: msg.id)
        
        if len(batch) > len(albums_in_batch) * 2:  # Простая проверка на разумный размер
            self.logger.debug(f"Батч расширен до {len(batch)} сообщений для сохранения целостности альбомов")
        
        return batch
    
    async def _process_message_batch(self, batch: List[Message], progress_tracker: ProgressTracker) -> Dict[str, int]:
        """
        Обрабатывает один батч сообщений в хронологическом порядке.
        ИСПРАВЛЕНО: Добавлена обработка комментариев для сохранения полной функциональности.
        
        Args:
            batch: Батч сообщений для обработки
            progress_tracker: Трекер прогресса
            
        Returns:
            Статистика обработки батча
        """
        batch_stats = {'copied': 0, 'failed': 0, 'skipped': 0}
        
        self.logger.debug(f"🔄 Начинаем обработку батча из {len(batch)} сообщений")
        
        # ИСПРАВЛЕНИЕ: Собираем комментарии для сообщений в батче (если нужно)
        batch_with_comments = []
        
        if self.flatten_structure:
            self.logger.debug("💬 Режим антивложенности: собираем комментарии для сообщений в батче")
            
            # Собираем комментарии для каждого сообщения в батче
            for message in batch:
                batch_with_comments.append(message)
                
                # НОВОЕ: Получаем комментарии из кэша
                try:
                    comments = await self.get_comments_from_cache(message)
                    if comments:
                        self.logger.debug(f"💬 Сообщение {message.id}: найдено {len(comments)} комментариев в кэше")
                        
                        # Помечаем комментарии специальным атрибутом
                        for comment in comments:
                            comment._is_from_discussion_group = True
                            comment._parent_message_id = message.id
                        
                        batch_with_comments.extend(comments)
                    else:
                        self.logger.debug(f"💬 Сообщение {message.id}: комментариев в кэше не найдено")
                        
                except Exception as e:
                    self.logger.warning(f"Ошибка получения комментариев из кэша для сообщения {message.id}: {e}")
            
            # Сортируем весь батч (сообщения + комментарии) по дате для правильной хронологии
            batch_with_comments.sort(key=lambda msg: msg.date if hasattr(msg, 'date') and msg.date else msg.id)
            self.logger.debug(f"📊 Батч расширен до {len(batch_with_comments)} элементов (включая комментарии)")
            
        else:
            # Если режим антивложенности выключен, просто используем исходный батч
            batch_with_comments = batch
            self.logger.debug("🔗 Режим с вложенностью: комментарии НЕ добавляются в батч")
        
        # Группируем альбомы в расширенном батче
        albums = {}  # grouped_id -> список сообщений
        single_messages = []
        
        for message in batch_with_comments:
            if hasattr(message, 'grouped_id') and message.grouped_id:
                if message.grouped_id not in albums:
                    albums[message.grouped_id] = []
                albums[message.grouped_id].append(message)
            else:
                single_messages.append(message)
        
        # Обрабатываем сообщения в порядке их ID (хронология)
        all_items = []
        
        # Добавляем одиночные сообщения
        for msg in single_messages:
            all_items.append(('single', msg))
        
        # Добавляем альбомы (берем первое сообщение альбома для сортировки)
        for grouped_id, album_messages in albums.items():
            album_messages.sort(key=lambda x: x.id)  # Сортируем внутри альбома
            all_items.append(('album', album_messages))
        
        # Сортируем по ID для сохранения хронологии
        all_items.sort(key=lambda item: item[1].id if item[0] == 'single' else item[1][0].id)
        
        self.logger.debug(f"📦 Обрабатываем {len(all_items)} элементов: {len(single_messages)} одиночных, {len(albums)} альбомов")
        
        # Обрабатываем каждый элемент
        for item_type, item_data in all_items:
            try:
                success = False
                if item_type == 'single':
                    # Одиночное сообщение
                    message = item_data
                    
                    # ИСПРАВЛЕНИЕ: Различная обработка для комментариев и основных сообщений
                    if hasattr(message, '_is_from_discussion_group') and message._is_from_discussion_group:
                        # Это комментарий из discussion group
                        self.logger.debug(f"💬 Копируем комментарий {message.id} (родительский пост: {getattr(message, '_parent_message_id', 'неизвестно')})")
                    else:
                        # Это основное сообщение
                        self.logger.debug(f"📝 Копируем основное сообщение {message.id}")
                    
                    success = await self.copy_single_message(message)
                    
                    if success:
                        batch_stats['copied'] += 1
                        save_last_message_id(message.id, self.resume_file)
                    else:
                        batch_stats['failed'] += 1
                    
                    progress_tracker.update(success)
                    
                    # НОВОЕ: Рекурсивная обработка комментариев в режиме с вложенностью
                    if not self.flatten_structure and success and not hasattr(message, '_is_from_discussion_group'):
                        # Используем рекурсивный метод для полной обработки комментариев
                        try:
                            self.logger.debug(f"🔗 Запуск рекурсивной обработки комментариев для сообщения {message.id}")
                            comments_stats = await self._process_comments_recursively(message)
                            
                            # Обновляем статистику батча
                            batch_stats['copied'] += comments_stats['copied']
                            batch_stats['failed'] += comments_stats['failed']
                            
                            # Обновляем трекер прогресса для всех комментариев
                            for _ in range(comments_stats['copied'] + comments_stats['failed']):
                                progress_tracker.update(True if comments_stats['copied'] > 0 else False)
                            
                            if comments_stats['copied'] > 0 or comments_stats['failed'] > 0:
                                self.logger.debug(f"📊 Рекурсивная обработка комментариев к сообщению {message.id} завершена: "
                                                f"скопировано {comments_stats['copied']}, ошибок {comments_stats['failed']}")
                                        
                        except Exception as e:
                            self.logger.warning(f"Ошибка рекурсивной обработки комментариев для сообщения {message.id}: {e}")
                
                elif item_type == 'album':
                    # Альбом
                    album_messages = item_data
                    
                    # Проверяем, является ли это альбомом из комментариев
                    is_comment_album = any(hasattr(msg, '_is_from_discussion_group') and msg._is_from_discussion_group 
                                         for msg in album_messages)
                    
                    if is_comment_album:
                        parent_id = getattr(album_messages[0], '_parent_message_id', 'неизвестно')
                        self.logger.debug(f"💬 Копируем альбом-комментарий из {len(album_messages)} сообщений (родительский пост: {parent_id})")
                    else:
                        self.logger.debug(f"📸 Копируем основной альбом из {len(album_messages)} сообщений")
                    
                    success = await self.copy_album(album_messages)
                    
                    if success:
                        batch_stats['copied'] += len(album_messages)
                        last_album_id = max(msg.id for msg in album_messages)
                        save_last_message_id(last_album_id, self.resume_file)
                    else:
                        batch_stats['failed'] += len(album_messages)
                    
                    for msg in album_messages:
                        progress_tracker.update(success)
                    
                    # НОВОЕ: Рекурсивная обработка комментариев к альбому в режиме с вложенностью
                    if not self.flatten_structure and success and not is_comment_album:
                        # Обрабатываем комментарии к альбому (берем первое сообщение альбома как представитель)
                        representative_message = album_messages[0]
                        try:
                            self.logger.debug(f"🔗 Запуск рекурсивной обработки комментариев для альбома {representative_message.grouped_id}")
                            comments_stats = await self._process_comments_recursively(representative_message)
                            
                            # Обновляем статистику батча
                            batch_stats['copied'] += comments_stats['copied']
                            batch_stats['failed'] += comments_stats['failed']
                            
                            # Обновляем трекер прогресса для всех комментариев
                            for _ in range(comments_stats['copied'] + comments_stats['failed']):
                                progress_tracker.update(True if comments_stats['copied'] > 0 else False)
                            
                            if comments_stats['copied'] > 0 or comments_stats['failed'] > 0:
                                self.logger.debug(f"📊 Рекурсивная обработка комментариев к альбому {representative_message.grouped_id} завершена: "
                                                f"скопировано {comments_stats['copied']}, ошибок {comments_stats['failed']}")
                                        
                        except Exception as e:
                            self.logger.warning(f"Ошибка рекурсивной обработки комментариев для альбома {representative_message.grouped_id}: {e}")
                
                # Соблюдаем лимиты скорости
                if not self.dry_run and success:
                    await self.rate_limiter.wait_if_needed()
                    self.rate_limiter.record_message_sent()
            
            except FloodWaitError as e:
                await handle_flood_wait(e, self.logger)
                # Повторяем попытку для текущего элемента
                if item_type == 'single':
                    success = await self.copy_single_message(item_data)
                    if success:
                        batch_stats['copied'] += 1
                    else:
                        batch_stats['failed'] += 1
                else:
                    success = await self.copy_album(item_data)
                    if success:
                        batch_stats['copied'] += len(item_data)
                    else:
                        batch_stats['failed'] += len(item_data)
                
            except Exception as e:
                self.logger.error(f"Ошибка обработки элемента {item_type}: {e}")
                if item_type == 'single':
                    batch_stats['failed'] += 1
                else:
                    batch_stats['failed'] += len(item_data)
        
        return batch_stats
    
    async def _cleanup_batch_memory(self):
        """Принудительная очистка памяти после обработки батча."""
        import gc
        gc.collect()  # Принудительный сбор мусора
        
        # Очищаем кэши дедупликатора (только если файл старый)
        self.deduplicator.cleanup_old_hashes()
        
        # Небольшая пауза для системы
        await asyncio.sleep(0.1)

    async def _process_comments_recursively(self, parent_message: Message, depth: int = 0, 
                                          max_depth: int = 10) -> Dict[str, int]:
        """
        ИСПРАВЛЕНИЕ: Рекурсивная обработка комментариев для восстановления полной функциональности.
        Обрабатывает комментарии к сообщению и комментарии к комментариям (любой уровень вложенности).
        
        Args:
            parent_message: Сообщение, для которого обрабатываем комментарии
            depth: Текущий уровень глубины (для предотвращения бесконечной рекурсии)
            max_depth: Максимальная глубина рекурсии
            
        Returns:
            Статистика обработки комментариев
        """
        comments_stats = {'copied': 0, 'failed': 0}
        
        if depth >= max_depth:
            self.logger.warning(f"Достигнута максимальная глубина рекурсии {max_depth} для комментариев")
            return comments_stats
        
        try:
            # НОВОЕ: Получаем комментарии из кэша
            comments = await self.get_comments_from_cache(parent_message)
            
            if not comments:
                self.logger.debug(f"{'  ' * depth}💬 Сообщение {parent_message.id}: комментариев не найдено (глубина {depth})")
                return comments_stats
            
            self.logger.debug(f"{'  ' * depth}🔗 Обрабатываем {len(comments)} комментариев к сообщению {parent_message.id} (глубина {depth})")
            
            # Сортируем комментарии по дате для сохранения хронологии
            comments.sort(key=lambda c: c.date if hasattr(c, 'date') and c.date else c.id)
            
            for comment in comments:
                try:
                    # Копируем комментарий
                    self.logger.debug(f"{'  ' * depth}💬 Копируем комментарий {comment.id} (глубина {depth})")
                    comment_success = await self.copy_single_message(comment)
                    
                    if comment_success:
                        comments_stats['copied'] += 1
                        self.logger.debug(f"{'  ' * depth}✅ Комментарий {comment.id} скопирован")
                        
                        # Рекурсивно обрабатываем комментарии к этому комментарию
                        nested_stats = await self._process_comments_recursively(comment, depth + 1, max_depth)
                        comments_stats['copied'] += nested_stats['copied']
                        comments_stats['failed'] += nested_stats['failed']
                        
                    else:
                        comments_stats['failed'] += 1
                        self.logger.debug(f"{'  ' * depth}❌ Ошибка копирования комментария {comment.id}")
                    
                    # Соблюдаем лимиты скорости
                    if not self.dry_run and comment_success:
                        await self.rate_limiter.wait_if_needed()
                        self.rate_limiter.record_message_sent()
                        
                except Exception as comment_error:
                    self.logger.error(f"{'  ' * depth}Ошибка обработки комментария {comment.id}: {comment_error}")
                    comments_stats['failed'] += 1
                    
        except Exception as e:
            self.logger.warning(f"{'  ' * depth}Ошибка получения комментариев для сообщения {parent_message.id} (глубина {depth}): {e}")
        
        if comments_stats['copied'] > 0 or comments_stats['failed'] > 0:
            self.logger.debug(f"{'  ' * depth}📊 Обработка комментариев завершена (глубина {depth}): "
                            f"скопировано {comments_stats['copied']}, ошибок {comments_stats['failed']}")
        
        return comments_stats
    
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
        ИСПРАВЛЕНА ПРОБЛЕМА С АЛЬБОМАМИ В КОММЕНТАРИЯХ: сохраняются атрибуты файлов.
        
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
            
            # Проверяем, нужно ли скачивать медиа (для комментариев из protected chats)
            is_from_discussion_group = hasattr(first_message, '_is_from_discussion_group') and first_message._is_from_discussion_group
            
            # Собираем все медиа файлы из альбома
            media_files = []
            for message in album_messages:
                if message.media:
                    if is_from_discussion_group:
                        # Для комментариев скачиваем медиа и создаем временный файл с правильным именем
                        self.logger.debug(f"Скачиваем медиа из комментария {message.id} для альбома с сохранением атрибутов")
                        
                        # Получаем информацию о файле из оригинального медиа
                        suggested_filename = None
                        
                        if hasattr(message.media, 'document') and message.media.document:
                            doc = message.media.document
                            
                            # Пытаемся извлечь имя файла из атрибутов
                            for attr in getattr(doc, 'attributes', []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    suggested_filename = attr.file_name
                                    break
                            
                            # Если имя файла не найдено, генерируем на основе MIME-типа
                            if not suggested_filename:
                                mime_type = getattr(doc, 'mime_type', None)
                                if mime_type:
                                    if mime_type.startswith('image/'):
                                        extension = mime_type.split('/')[-1]
                                        if extension == 'jpeg':
                                            extension = 'jpg'
                                        suggested_filename = f"image_{message.id}.{extension}"
                                    elif mime_type.startswith('video/'):
                                        extension = mime_type.split('/')[-1]
                                        suggested_filename = f"video_{message.id}.{extension}"
                                    elif mime_type.startswith('audio/'):
                                        extension = mime_type.split('/')[-1]
                                        suggested_filename = f"audio_{message.id}.{extension}"
                                    else:
                                        suggested_filename = f"document_{message.id}"
                                else:
                                    suggested_filename = f"document_{message.id}"
                        
                        elif isinstance(message.media, MessageMediaPhoto):
                            suggested_filename = f"photo_{message.id}.jpg"
                        
                        if not suggested_filename:
                            suggested_filename = f"media_{message.id}"
                        
                        # Скачиваем файл как bytes
                        downloaded_file = await self.client.download_media(message.media, file=bytes)
                        
                        # Создаем временный файл в памяти с правильным именем
                        import tempfile
                        import os
                        
                        # Создаем временный файл с правильным именем
                        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{suggested_filename}") as temp_file:
                            temp_file.write(downloaded_file)
                            temp_file_path = temp_file.name
                        
                        # Переименовываем файл для правильного имени
                        proper_temp_path = os.path.join(os.path.dirname(temp_file_path), suggested_filename)
                        os.rename(temp_file_path, proper_temp_path)
                        
                        media_files.append(proper_temp_path)
                    else:
                        # Для основных сообщений используем прямую ссылку
                        media_files.append(message.media)
            
            if not media_files:
                self.logger.warning("Альбом не содержит медиа файлов")
                # Если нет медиа, отправляем как обычное текстовое сообщение
                if first_message.message:
                    return await self.copy_single_message(first_message)
                return False
            
            # ИСПРАВЛЕНИЕ: Получаем текст из ЛЮБОГО сообщения альбома, где он есть
            caption = ""
            caption_entities = None
            caption_message_id = None
            
            # Проверяем все сообщения альбома на наличие текста
            for msg in album_messages:
                if msg.message and msg.message.strip():
                    caption = msg.message
                    caption_entities = msg.entities
                    caption_message_id = msg.id
                    self.logger.debug(f"Найден текст альбома в сообщении {msg.id}: '{caption[:50]}...'")
                    break  # Берем первый найденный непустой текст
            
            # Если текста нет, но включен debug режим, добавляем ID первого сообщения альбома
            if not caption and self.debug_message_ids:
                caption_message_id = album_messages[0].id
            
            # Добавляем debug ID к тексту альбома
            if caption_message_id:
                caption = self._add_debug_id_to_text(caption, caption_message_id)
            
            if not caption and not self.debug_message_ids:
                self.logger.debug("Альбом без текста")
            
            # Подготавливаем параметры для отправки альбома
            send_kwargs = {
                'entity': self.target_entity,
                'file': media_files,  # Массив медиа файлов (скачанных с именами или ссылок)
                'caption': caption,
            }
            
            # ВАЖНО: Сохраняем форматирование текста из сообщения с текстом
            if caption_entities:
                send_kwargs['formatting_entities'] = caption_entities
            
            self.logger.debug(f"Отправляем альбом из {len(media_files)} медиа файлов")
            
            # Отправляем альбом как группированные медиа
            sent_messages = await self.client.send_file(**send_kwargs)
            
            # ВАЖНО: Очищаем временные файлы после отправки
            temp_files_to_cleanup = []
            for media_file in media_files:
                if isinstance(media_file, str) and os.path.exists(media_file) and media_file.startswith('/tmp'):
                    temp_files_to_cleanup.append(media_file)
            
            for temp_file in temp_files_to_cleanup:
                try:
                    os.remove(temp_file)
                    self.logger.debug(f"Удален временный файл: {temp_file}")
                except Exception as e:
                    self.logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")
            
            # Анализируем результат
            if isinstance(sent_messages, list):
                self.logger.info(f"✅ Альбом успешно отправлен как {len(sent_messages)} сообщений с правильными именами файлов")
                
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
            # Пытаемся отправить только текст из любого сообщения альбома, где он есть
            text_message = None
            for msg in album_messages:
                if msg.message and msg.message.strip():
                    text_message = msg
                    break
            
            if text_message:
                try:
                    text_kwargs = {
                        'entity': self.target_entity,
                        'message': text_message.message,
                        'link_preview': False
                    }
                    if text_message.entities:
                        text_kwargs['formatting_entities'] = text_message.entities
                    await self.client.send_message(**text_kwargs)
                    self.logger.info(f"Отправлен только текст альбома (медиа недоступно)")
                    return True
                except Exception as text_error:
                    self.logger.error(f"Ошибка отправки текста альбома: {text_error}")
            return False
            
        except Exception as e:
            self.logger.error(f"Ошибка копирования альбома: {e}")
            
            # Очищаем временные файлы в случае ошибки
            if 'media_files' in locals():
                for media_file in media_files:
                    if isinstance(media_file, str) and os.path.exists(media_file) and media_file.startswith('/tmp'):
                        try:
                            os.remove(media_file)
                            self.logger.debug(f"Удален временный файл после ошибки: {media_file}")
                        except Exception as cleanup_error:
                            self.logger.warning(f"Не удалось удалить временный файл {media_file}: {cleanup_error}")
            
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
            
            # Добавляем debug ID к тексту сообщения
            text = self._add_debug_id_to_text(text, message.id)
            
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
                # Проверяем, нужно ли скачивать медиа (для protected chats)
                is_from_discussion_group = hasattr(message, '_is_from_discussion_group') and message._is_from_discussion_group
                
                if isinstance(message.media, MessageMediaPhoto):
                    # Для фотографий
                    if is_from_discussion_group:
                        # Для комментариев скачиваем и создаем временный файл с правильным именем
                        self.logger.debug(f"Скачиваем фото из комментария {message.id} для повторной загрузки с сохранением имени")
                        downloaded_file = await self.client.download_media(message.media, file=bytes)
                        
                        # Создаем временный файл с правильным именем
                        suggested_filename = f"photo_{message.id}.jpg"
                        import tempfile
                        import os
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{suggested_filename}") as temp_file:
                            temp_file.write(downloaded_file)
                            temp_file_path = temp_file.name
                        
                        proper_temp_path = os.path.join(os.path.dirname(temp_file_path), suggested_filename)
                        os.rename(temp_file_path, proper_temp_path)
                        
                        file_kwargs = {
                            'entity': self.target_entity,
                            'file': proper_temp_path,
                            'caption': text,
                            'force_document': False
                        }
                        
                        # Удаляем временный файл после отправки будет ниже
                        temp_file_to_cleanup = proper_temp_path
                    else:
                        # Для основных сообщений используем прямую ссылку
                        file_kwargs = {
                            'entity': self.target_entity,
                            'file': message.media,
                            'caption': text,
                            'force_document': False
                        }
                        temp_file_to_cleanup = None
                    
                    if message.entities:
                        file_kwargs['formatting_entities'] = message.entities
                    sent_message = await self.client.send_file(**file_kwargs)
                    
                    # Удаляем временный файл если он был создан
                    if temp_file_to_cleanup:
                        try:
                            os.remove(temp_file_to_cleanup)
                        except Exception as e:
                            self.logger.warning(f"Не удалось удалить временный файл {temp_file_to_cleanup}: {e}")
                    
                elif isinstance(message.media, MessageMediaDocument):
                    # Для документов/видео/аудио
                    if is_from_discussion_group:
                        # Для комментариев скачиваем и создаем временный файл с правильным именем
                        self.logger.debug(f"Скачиваем документ из комментария {message.id} для повторной загрузки с сохранением имени")
                        downloaded_file = await self.client.download_media(message.media, file=bytes)
                        
                        # Определяем правильное имя файла
                        suggested_filename = None
                        if hasattr(message.media, 'document') and message.media.document:
                            doc = message.media.document
                            # Пытаемся извлечь имя файла из атрибутов
                            for attr in getattr(doc, 'attributes', []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    suggested_filename = attr.file_name
                                    break
                            
                            # Если имя файла не найдено, генерируем на основе MIME-типа
                            if not suggested_filename:
                                mime_type = getattr(doc, 'mime_type', None)
                                if mime_type:
                                    if mime_type.startswith('image/'):
                                        extension = mime_type.split('/')[-1]
                                        if extension == 'jpeg':
                                            extension = 'jpg'
                                        suggested_filename = f"image_{message.id}.{extension}"
                                    elif mime_type.startswith('video/'):
                                        extension = mime_type.split('/')[-1]
                                        suggested_filename = f"video_{message.id}.{extension}"
                                    elif mime_type.startswith('audio/'):
                                        extension = mime_type.split('/')[-1]
                                        suggested_filename = f"audio_{message.id}.{extension}"
                                    else:
                                        suggested_filename = f"document_{message.id}"
                                else:
                                    suggested_filename = f"document_{message.id}"
                        
                        if not suggested_filename:
                            suggested_filename = f"document_{message.id}"
                        
                        # Создаем временный файл с правильным именем
                        import tempfile
                        import os
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{suggested_filename}") as temp_file:
                            temp_file.write(downloaded_file)
                            temp_file_path = temp_file.name
                        
                        proper_temp_path = os.path.join(os.path.dirname(temp_file_path), suggested_filename)
                        os.rename(temp_file_path, proper_temp_path)
                        
                        file_kwargs = {
                            'entity': self.target_entity,
                            'file': proper_temp_path,
                            'caption': text,
                            'force_document': True
                        }
                        temp_file_to_cleanup = proper_temp_path
                    else:
                        # Для основных сообщений используем прямую ссылку
                        file_kwargs = {
                            'entity': self.target_entity,
                            'file': message.media,
                            'caption': text,
                            'force_document': True
                        }
                        temp_file_to_cleanup = None
                    
                    if message.entities:
                        file_kwargs['formatting_entities'] = message.entities
                    sent_message = await self.client.send_file(**file_kwargs)
                    
                    # Удаляем временный файл если он был создан
                    if temp_file_to_cleanup:
                        try:
                            os.remove(temp_file_to_cleanup)
                        except Exception as e:
                            self.logger.warning(f"Не удалось удалить временный файл {temp_file_to_cleanup}: {e}")
                elif isinstance(message.media, MessageMediaWebPage):
                    # Для веб-страниц отправляем только текст с entities
                    sent_message = await self.client.send_message(**send_kwargs)
                else:
                    # Для других типов медиа пытаемся отправить как есть
                    try:
                        if is_from_discussion_group:
                            # Для комментариев скачиваем и загружаем заново
                            self.logger.debug(f"Скачиваем медиа типа {type(message.media)} из комментария {message.id}")
                            downloaded_file = await self.client.download_media(message.media, file=bytes)
                            
                            file_kwargs = {
                                'entity': self.target_entity,
                                'file': downloaded_file,
                                'caption': text
                            }
                        else:
                            # Для основных сообщений используем прямую ссылку
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
    
    async def preload_all_comments_cache(self, sample_batch: List[Message]) -> None:
        """
        НОВЫЙ ПОДХОД: Предварительно загружает ВСЕ комментарии из ВСЕХ discussion groups канала
        для батчевой обработки. Использует первый батч для определения discussion groups.
        
        Args:
            sample_batch: Первый батч сообщений для определения всех discussion groups канала
        """
        if self.comments_cache_loaded:
            return  # Кэш уже загружен
        
        try:
            self.logger.info(f"🔄 ГЛОБАЛЬНАЯ ПРЕДВАРИТЕЛЬНАЯ ЗАГРУЗКА: Сканируем канал для поиска discussion groups...")
            
            # ЭТАП 1: Сканируем ВЕСЬ канал для поиска всех discussion groups
            discussion_groups = set()
            scanned_messages = 0
            
            # Сканируем большую выборку сообщений для полного поиска discussion groups
            async for message in self.client.iter_messages(self.source_entity, limit=5000):
                scanned_messages += 1
                
                if (hasattr(message, 'replies') and message.replies and
                    hasattr(message.replies, 'comments') and message.replies.comments and
                    hasattr(message.replies, 'channel_id') and message.replies.channel_id):
                    discussion_groups.add(message.replies.channel_id)
                
                # Логируем прогресс каждые 1000 сообщений
                if scanned_messages % 1000 == 0:
                    self.logger.info(f"   📊 Просканировано {scanned_messages} сообщений, найдено {len(discussion_groups)} discussion groups")
            
            self.logger.info(f"📊 СКАНИРОВАНИЕ ЗАВЕРШЕНО: просканировано {scanned_messages} сообщений, "
                           f"найдено {len(discussion_groups)} уникальных discussion groups")
            
            if not discussion_groups:
                self.logger.info("💬 Discussion groups не найдены в канале")
                self.comments_cache_loaded = True
                return
            
            # ЭТАП 2: Загружаем ВСЕ комментарии из ВСЕХ найденных discussion groups
            total_comments = 0
            total_posts_with_comments = 0
            
            for discussion_group_id in discussion_groups:
                self.logger.info(f"📥 Загружаем ВСЕ комментарии из discussion group {discussion_group_id}")
                
                try:
                    comments_by_post = await self.get_all_comments_from_discussion_group(discussion_group_id)
                    
                    # Добавляем в общий кэш
                    for post_id, comments in comments_by_post.items():
                        if post_id not in self.comments_cache:
                            self.comments_cache[post_id] = []
                            total_posts_with_comments += 1
                        self.comments_cache[post_id].extend(comments)
                        total_comments += len(comments)
                    
                    # Помечаем группу как загруженную
                    self.discussion_groups_cache.add(discussion_group_id)
                    
                    self.logger.info(f"✅ Загружено {len(comments_by_post)} связей пост->комментарии из группы {discussion_group_id}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка загрузки комментариев из группы {discussion_group_id}: {e}")
            
            self.comments_cache_loaded = True
            self.logger.info(f"🎯 ГЛОБАЛЬНАЯ ПРЕДВАРИТЕЛЬНАЯ ЗАГРУЗКА ЗАВЕРШЕНА:")
            self.logger.info(f"   📊 Загружено {total_comments} комментариев")
            self.logger.info(f"   📊 Для {total_posts_with_comments} постов с комментариями")
            self.logger.info(f"   📊 Из {len(discussion_groups)} discussion groups")
            self.logger.info(f"   🚀 Теперь все батчи будут использовать кэш!")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка предварительной загрузки комментариев: {e}")
    
    async def get_comments_from_cache(self, message: Message) -> List[Message]:
        """
        НОВЫЙ ПОДХОД: Получает комментарии для сообщения из предварительно загруженного кэша.
        
        Args:
            message: Сообщение из канала, для которого нужно получить комментарии
            
        Returns:
            Список сообщений-комментариев из кэша
        """
        try:
            comments = self.comments_cache.get(message.id, [])
            if comments:
                self.logger.debug(f"💬 Сообщение {message.id}: найдено {len(comments)} комментариев в кэше")
            else:
                self.logger.debug(f"💬 Сообщение {message.id}: комментариев в кэше не найдено")
            return comments
            
        except Exception as e:
            self.logger.warning(f"Ошибка получения комментариев из кэша для сообщения {message.id}: {e}")
            return []