"""
Модуль для обработки групповых медиа (альбомов) в Telegram.
Обеспечивает правильную группировку и отправку альбомов фотографий.
"""

import logging
from typing import List, Dict, Any, Optional
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from telethon import TelegramClient


class AlbumHandler:
    """Класс для обработки групповых медиа (альбомов)."""
    
    def __init__(self, client: TelegramClient):
        """
        Инициализация обработчика альбомов.
        
        Args:
            client: Авторизованный Telegram клиент
        """
        self.client = client
        self.logger = logging.getLogger('telegram_copier.album_handler')
        self.pending_albums: Dict[int, List[Message]] = {}
    
    def is_album_message(self, message: Message) -> bool:
        """
        Проверка, является ли сообщение частью альбома.
        
        Args:
            message: Сообщение для проверки
        
        Returns:
            True если сообщение является частью альбома
        """
        return (hasattr(message, 'grouped_id') and 
                message.grouped_id is not None and 
                message.media is not None)
    
    def add_message_to_album(self, message: Message) -> Optional[List[Message]]:
        """
        Добавление сообщения в альбом и возврат готового альбома.
        
        Args:
            message: Сообщение для добавления
        
        Returns:
            Список сообщений альбома, если альбом готов к отправке, иначе None
        """
        if not self.is_album_message(message):
            return [message]  # Одиночное сообщение
        
        grouped_id = message.grouped_id
        
        # Добавляем сообщение в соответствующий альбом
        if grouped_id not in self.pending_albums:
            self.pending_albums[grouped_id] = []
        
        self.pending_albums[grouped_id].append(message)
        
        # Проверяем, готов ли альбом (обычно альбомы содержат 2-10 элементов)
        album_messages = self.pending_albums[grouped_id]
        
        # Простая эвристика: если прошло время или достигнут максимум
        if len(album_messages) >= 10:  # Максимум элементов в альбоме
            completed_album = self.pending_albums.pop(grouped_id)
            self.logger.debug(f"Альбом {grouped_id} готов к отправке ({len(completed_album)} элементов)")
            return completed_album
        
        return None  # Альбом еще не готов
    
    def finalize_pending_albums(self) -> List[List[Message]]:
        """
        Завершение обработки всех ожидающих альбомов.
        
        Returns:
            Список готовых альбомов
        """
        completed_albums = []
        
        for grouped_id, messages in self.pending_albums.items():
            if messages:
                self.logger.debug(f"Завершаем альбом {grouped_id} ({len(messages)} элементов)")
                completed_albums.append(messages)
        
        self.pending_albums.clear()
        return completed_albums
    
    async def send_album(self, target_entity, album_messages: List[Message]) -> bool:
        """
        Отправка альбома в целевую группу/канал.
        ОБНОВЛЕНО: Улучшенная обработка альбомов с сохранением форматирования.
        
        Args:
            target_entity: Целевая группа/канал
            album_messages: Список сообщений альбома (должен быть отсортирован по ID)
        
        Returns:
            True если отправка успешна
        """
        try:
            if not album_messages:
                return False
            
            # Сортируем сообщения по ID для правильного порядка
            album_messages.sort(key=lambda x: x.id)
            
            # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Скачиваем медиа файлы для избежания ошибки "protected chat"
            downloaded_files = []
            first_message = album_messages[0]
            
            for i, message in enumerate(album_messages):
                if message.media:
                    try:
                        # Скачиваем медиа файл в память
                        self.logger.debug(f"Скачиваем медиа файл {i+1}/{len(album_messages)} из сообщения {message.id}")
                        
                        file_bytes = await self.client.download_media(message.media, file=bytes)
                        
                        if file_bytes:
                            downloaded_files.append(file_bytes)
                            self.logger.debug(f"Успешно скачан файл {i+1}: {len(file_bytes)} байт")
                        else:
                            self.logger.warning(f"Не удалось скачать медиа из сообщения {message.id}")
                            
                    except Exception as download_error:
                        self.logger.warning(f"Ошибка скачивания медиа из сообщения {message.id}: {download_error}")
                        continue
            
            if not downloaded_files:
                self.logger.warning("Альбом не содержит доступных медиа файлов")
                # Если нет медиа, отправляем текст из первого сообщения
                if first_message.message:
                    return await self.send_single_message(target_entity, first_message)
                return False
            
            # ИСПРАВЛЕНО: Получаем текст из любого сообщения альбома
            caption, entities = self.extract_album_text(album_messages)
            
            # Подготавливаем параметры отправки со скачанными файлами
            send_kwargs = {
                'entity': target_entity,
                'file': downloaded_files,  # Используем скачанные байты
                'caption': caption,
            }
            
            # ВАЖНО: Сохраняем форматирование текста
            if entities:
                send_kwargs['formatting_entities'] = entities
            
            # Отправляем как группированные медиа
            sent_messages = await self.client.send_file(**send_kwargs)
            
            # Логируем результат
            if isinstance(sent_messages, list):
                self.logger.info(f"Альбом из {len(downloaded_files)} элементов успешно отправлен как {len(sent_messages)} сообщений")
            else:
                self.logger.info(f"Альбом из {len(downloaded_files)} элементов успешно отправлен")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка отправки альбома: {e}")
            return False
    
    async def send_single_message(self, target_entity, message: Message) -> bool:
        """
        Отправка одиночного сообщения (не альбом).
        
        Args:
            target_entity: Целевая группа/канал
            message: Сообщение для отправки
        
        Returns:
            True если отправка успешна
        """
        try:
            text = message.message or ""
            
            if message.media:
                try:
                    # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Скачиваем медиа для избежания ошибки "protected chat"
                    self.logger.debug(f"Скачиваем медиа из сообщения {message.id}")
                    
                    file_bytes = await self.client.download_media(message.media, file=bytes)
                    
                    if file_bytes:
                        # Отправляем скачанный медиа файл с подписью
                        file_kwargs = {
                            'entity': target_entity,
                            'file': file_bytes,  # Используем скачанные байты
                            'caption': text
                        }
                        
                        if message.entities:
                            file_kwargs['formatting_entities'] = message.entities
                        
                        # Определяем тип медиа для правильной отправки
                        if isinstance(message.media, MessageMediaDocument):
                            file_kwargs['force_document'] = True
                        
                        await self.client.send_file(**file_kwargs)
                    else:
                        # Если не удалось скачать медиа, отправляем только текст
                        self.logger.warning(f"Не удалось скачать медиа из сообщения {message.id}, отправляем только текст")
                        send_kwargs = {
                            'entity': target_entity,
                            'message': text,
                            'link_preview': False
                        }
                        
                        if message.entities:
                            send_kwargs['formatting_entities'] = message.entities
                        
                        await self.client.send_message(**send_kwargs)
                        
                except Exception as media_error:
                    self.logger.warning(f"Ошибка обработки медиа из сообщения {message.id}: {media_error}")
                    # Отправляем только текст в случае ошибки
                    send_kwargs = {
                        'entity': target_entity,
                        'message': text,
                        'link_preview': False
                    }
                    
                    if message.entities:
                        send_kwargs['formatting_entities'] = message.entities
                    
                    await self.client.send_message(**send_kwargs)
            else:
                # Отправляем текстовое сообщение
                send_kwargs = {
                    'entity': target_entity,
                    'message': text,
                    'link_preview': False
                }
                
                if message.entities:
                    send_kwargs['formatting_entities'] = message.entities
                
                await self.client.send_message(**send_kwargs)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка отправки сообщения: {e}")
            return False
    
    def extract_album_text(self, album_messages: List[Message]) -> tuple[str, Optional[List]]:
        """
        Извлечение текста и форматирования из любого сообщения альбома.
        ИСПРАВЛЕНО: Теперь проверяет все сообщения альбома, а не только первое.
        
        Args:
            album_messages: Список сообщений альбома
        
        Returns:
            Кортеж (text, entities) где text - текст сообщения, entities - форматирование
        """
        # Проверяем все сообщения альбома на наличие текста
        for message in album_messages:
            if message.message and message.message.strip():
                self.logger.debug(f"Найден текст в сообщении {message.id}: {message.message[:50]}...")
                return message.message, message.entities
        
        # Если текст не найден ни в одном сообщении
        self.logger.debug("Текст не найден ни в одном сообщении альбома")
        return "", None

    def get_album_stats(self) -> Dict[str, int]:
        """
        Получение статистики по альбомам.
        
        Returns:
            Статистика альбомов
        """
        return {
            'pending_albums': len(self.pending_albums),
            'pending_messages': sum(len(messages) for messages in self.pending_albums.values())
        }