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
    ChannelParticipantAdmin, ChannelParticipantCreator
)
from telethon.errors import FloodWaitError, PeerFloodError, MediaInvalidError
from telethon.tl import functions
from telethon.tl.functions.channels import GetParticipantRequest
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
                 add_debug_tags: bool = False):
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
                            participant = await self.client(GetParticipantRequest(
                                channel=self.target_entity,
                                user_id=me.id
                            ))
                            
                            is_admin = isinstance(participant.participant, (ChannelParticipantAdmin, ChannelParticipantCreator))
                            
                            if is_admin:
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
    
    async def copy_all_messages(self, resume_from_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Копирование всех сообщений из исходной группы/канала в целевую группу/канал.
        
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
        
        # Определяем начальную позицию
        if self.message_tracker and not resume_from_id:
            # Используем трекер для определения последнего ID
            last_copied_id = self.message_tracker.get_last_copied_id()
            if last_copied_id:
                resume_from_id = last_copied_id
                self.logger.info(f"📊 Трекер: последний скопированный ID {last_copied_id}")
        
        min_id = resume_from_id if resume_from_id else 0
        
        try:
            # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Потоковая обработка сообщений
            # Вместо загрузки всех сообщений в память, обрабатываем их по одному
            self.logger.info("Начинаем потоковую обработку сообщений")
            
            # Определяем параметры для iter_messages
            iter_params = {
                'entity': self.source_entity,
                'reverse': True,  # От старых к новым
                'limit': None     # Все сообщения
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
            
            # Инициализируем трекер прогресса с приблизительным количеством
            progress_tracker = ProgressTracker(total_messages)
            message_count = 0
            
            # ИСПРАВЛЕННАЯ ОБРАБОТКА: Сначала собираем все сообщения с группировкой альбомов
            messages_to_process = []
            grouped_messages = {}  # grouped_id -> список сообщений
            
            # Собираем все сообщения
            async for message in self.client.iter_messages(**iter_params):
                message_count += 1
                
                # НОВОЕ: Проверка дедупликации
                if self.deduplicator.is_message_processed(message):
                    self.logger.debug(f"Сообщение {message.id} уже было обработано ранее, пропускаем")
                    self.skipped_messages += 1
                    continue
                
                # Проверяем, является ли сообщение частью альбома
                if hasattr(message, 'grouped_id') and message.grouped_id:
                    # Это сообщение является частью альбома
                    if message.grouped_id not in grouped_messages:
                        grouped_messages[message.grouped_id] = []
                    grouped_messages[message.grouped_id].append(message)
                    self.logger.debug(f"Добавлено сообщение {message.id} в альбом {message.grouped_id}")
                else:
                    # Обычное сообщение
                    messages_to_process.append(message)
            
            self.logger.info(f"Собрано {len(messages_to_process)} одиночных сообщений и {len(grouped_messages)} альбомов")
            
            # Теперь обрабатываем собранные альбомы
            for grouped_id, album_messages in grouped_messages.items():
                # Сортируем сообщения альбома по ID для правильного порядка
                album_messages.sort(key=lambda x: x.id)
                
                try:
                    self.logger.info(f"Обрабатываем альбом {grouped_id} из {len(album_messages)} сообщений")
                    
                    # Вычисляем общий размер альбома для мониторинга
                    total_size = 0
                    for msg in album_messages:
                        if msg.media and hasattr(msg.media, 'document') and msg.media.document:
                            total_size += getattr(msg.media.document, 'size', 0)
                        elif msg.message:
                            total_size += len(msg.message.encode('utf-8'))
                    
                    # Копируем альбом как единое целое
                    success = await self.copy_album(album_messages)
                    
                    # Обновляем статистику для всех сообщений альбома
                    for msg in album_messages:
                        progress_tracker.update(success)
                        self.performance_monitor.record_message_processed(success, total_size // len(album_messages))
                    
                    if success:
                        self.copied_messages += len(album_messages)
                        self.logger.info(f"Альбом {grouped_id} успешно скопирован")
                        # ИСПРАВЛЕНИЕ: Записываем ID только ПОСЛЕ успешного копирования
                        # Записываем ID последнего сообщения альбома
                        last_album_message_id = max(msg.id for msg in album_messages)
                        save_last_message_id(last_album_message_id, self.resume_file)
                        self.logger.debug(f"Записан ID {last_album_message_id} после успешного копирования альбома")
                    else:
                        self.failed_messages += len(album_messages)
                        self.logger.warning(f"Не удалось скопировать альбом {grouped_id}")
                        # При неудаче НЕ записываем ID
                    
                    # Соблюдаем лимиты скорости
                    if not self.dry_run:
                        await self.rate_limiter.wait_if_needed()
                        if success:
                            self.rate_limiter.record_message_sent()
                
                except FloodWaitError as e:
                    await handle_flood_wait(e, self.logger)
                    # Повторяем попытку копирования альбома
                    success = await self.copy_album(album_messages)
                    
                    for msg in album_messages:
                        progress_tracker.update(success)
                    
                    if success:
                        self.copied_messages += len(album_messages)
                        self.logger.info(f"Альбом {grouped_id} успешно скопирован после FloodWait")
                        # ИСПРАВЛЕНИЕ: Записываем ID только ПОСЛЕ успешного копирования
                        last_album_message_id = max(msg.id for msg in album_messages)
                        save_last_message_id(last_album_message_id, self.resume_file)
                        self.logger.debug(f"Записан ID {last_album_message_id} после успешного копирования альбома (FloodWait)")
                        if not self.dry_run:
                            self.rate_limiter.record_message_sent()
                    else:
                        self.failed_messages += len(album_messages)
                        self.logger.warning(f"Не удалось скопировать альбом {grouped_id} даже после FloodWait")
                
                except (PeerFloodError, MediaInvalidError) as e:
                    self.logger.warning(f"Telegram API ошибка для альбома {grouped_id}: {e}")
                    self.failed_messages += len(album_messages)
                    for msg in album_messages:
                        progress_tracker.update(False)
                
                except Exception as e:
                    self.logger.error(f"Неожиданная ошибка копирования альбома {grouped_id}: {type(e).__name__}: {e}")
                    self.failed_messages += len(album_messages)
                    for msg in album_messages:
                        progress_tracker.update(False)
            
            # Теперь обрабатываем одиночные сообщения
            for message in messages_to_process:
                try:
                    # Вычисляем размер сообщения для мониторинга
                    message_size = 0
                    if message.media and hasattr(message.media, 'document') and message.media.document:
                        message_size = getattr(message.media.document, 'size', 0)
                    elif message.message:
                        message_size = len(message.message.encode('utf-8'))
                    
                    # Копируем сообщение
                    success = await self.copy_single_message(message)
                    progress_tracker.update(success)
                    
                    # Записываем в мониторинг производительности
                    self.performance_monitor.record_message_processed(success, message_size)
                    
                    if success:
                        self.copied_messages += 1
                        self.logger.debug(f"Сообщение {message.id} успешно скопировано")
                        # ИСПРАВЛЕНИЕ: Записываем ID только ПОСЛЕ успешного копирования
                        save_last_message_id(message.id, self.resume_file)
                        self.logger.debug(f"Записан ID {message.id} после успешного копирования")
                    else:
                        self.failed_messages += 1
                        self.logger.warning(f"Не удалось скопировать сообщение {message.id}")
                        # При неудаче НЕ записываем ID
                    
                    # Соблюдаем лимиты скорости
                    if not self.dry_run:
                        await self.rate_limiter.wait_if_needed()
                        if success:
                            self.rate_limiter.record_message_sent()
                    
                except FloodWaitError as e:
                    await handle_flood_wait(e, self.logger)
                    success = await self.copy_single_message(message)
                    progress_tracker.update(success)
                    
                    if success:
                        self.copied_messages += 1
                        self.logger.debug(f"Сообщение {message.id} успешно скопировано после FloodWait")
                        # ИСПРАВЛЕНИЕ: Записываем ID только ПОСЛЕ успешного копирования
                        save_last_message_id(message.id, self.resume_file)
                        self.logger.debug(f"Записан ID {message.id} после успешного копирования (FloodWait)")
                        if not self.dry_run:
                            self.rate_limiter.record_message_sent()
                    else:
                        self.failed_messages += 1
                        self.logger.warning(f"Не удалось скопировать сообщение {message.id} даже после FloodWait")
                    
                except (PeerFloodError, MediaInvalidError) as e:
                    self.logger.warning(f"Telegram API ошибка для сообщения {message.id}: {e}")
                    self.failed_messages += 1
                    progress_tracker.update(False)
                    
                except Exception as e:
                    self.logger.error(f"Неожиданная ошибка копирования сообщения {message.id}: {type(e).__name__}: {e}")
                    self.failed_messages += 1
                    progress_tracker.update(False)
            
            self.logger.info(f"Обработано {message_count} сообщений")
        
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
        
        self.logger.info(f"Копирование завершено. Скопировано: {self.copied_messages}, "
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
            
            # Отправляем альбом как группированные медиа
            sent_messages = await self.client.send_file(**send_kwargs)
            
            # sent_messages может быть списком или одним сообщением
            if isinstance(sent_messages, list):
                self.logger.info(f"Альбом успешно отправлен как {len(sent_messages)} сообщений")
            else:
                self.logger.info(f"Альбом успешно отправлен как сообщение {sent_messages.id}")
            
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
    
    async def _prepare_media(self, message: Message) -> Optional[Union[str, bytes]]:
        """
        Подготовка медиа для отправки.
        
        Args:
            message: Сообщение с медиа
        
        Returns:
            Подготовленное медиа или None
        """
        try:
            if isinstance(message.media, MessageMediaPhoto):
                # Для фотографий возвращаем объект медиа напрямую
                return message.media
                
            elif isinstance(message.media, MessageMediaDocument):
                # Для документов тоже возвращаем объект медиа
                document = message.media.document
                
                # Проверяем размер файла (ограничение Telegram - 50MB для ботов, 2GB для клиентов)
                if hasattr(document, 'size') and document.size > 2 * 1024 * 1024 * 1024:  # 2GB
                    self.logger.warning(f"Файл слишком большой: {format_file_size(document.size)}")
                    return None
                
                return message.media
                
            elif isinstance(message.media, MessageMediaWebPage):
                # Веб-страницы не копируем как медиа, только как текст
                return None
                
            else:
                self.logger.debug(f"Неподдерживаемый тип медиа: {type(message.media)}")
                return None
                
        except Exception as e:
            self.logger.error(f"Ошибка подготовки медиа: {e}")
            return None
    
    async def _download_media(self, message: Message) -> Optional[str]:
        """
        Скачивание медиа файла (используется при необходимости).
        
        Args:
            message: Сообщение с медиа
        
        Returns:
            Путь к скачанному файлу или None
        """
        try:
            # Создаем директорию для временных файлов
            temp_dir = "temp_media"
            os.makedirs(temp_dir, exist_ok=True)
            
            # Генерируем имя файла
            file_name = f"message_{message.id}"
            
            if isinstance(message.media, MessageMediaDocument):
                document = message.media.document
                if hasattr(document, 'attributes'):
                    for attr in document.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            file_name = sanitize_filename(attr.file_name)
                            break
            
            file_path = os.path.join(temp_dir, file_name)
            
            # Скачиваем файл
            await self.client.download_media(message, file_path)
            
            return file_path
            
        except Exception as e:
            self.logger.error(f"Ошибка скачивания медиа: {e}")
            return None
    
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