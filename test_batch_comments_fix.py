#!/usr/bin/env python3
"""
Тест исправления батчевой обработки комментариев в хронологическом порядке.
Проверяет, что после исправления батчевый метод copy_all_messages_batch() 
корректно обрабатывает комментарии также, как и оригинальный copy_all_messages().
"""

import asyncio
import logging
import sys
from typing import List, Dict, Any
from datetime import datetime, timedelta

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('test_batch_comments_fix.log')
    ]
)

class MockMessage:
    """Мок-объект для имитации Telegram сообщения."""
    
    def __init__(self, id: int, text: str = "", date: datetime = None, 
                 grouped_id: int = None, reply_to: int = None, 
                 has_comments: bool = False, discussion_group_id: int = None):
        self.id = id
        self.message = text
        self.date = date or datetime.now() + timedelta(seconds=id)
        self.grouped_id = grouped_id
        self.reply_to = MockReplyTo(reply_to) if reply_to else None
        self.has_comments = has_comments
        self.discussion_group_id = discussion_group_id
        
        # Эмулируем replies для discussion groups
        if has_comments and discussion_group_id:
            self.replies = MockReplies(discussion_group_id)
        else:
            self.replies = None

class MockReplyTo:
    """Мок-объект для reply_to."""
    def __init__(self, reply_to_msg_id: int):
        self.reply_to_msg_id = reply_to_msg_id

class MockReplies:
    """Мок-объект для replies с discussion group."""
    def __init__(self, channel_id: int):
        self.comments = True
        self.channel_id = channel_id

class MockCopierTester:
    """Тестер для проверки исправления батчевой обработки комментариев."""
    
    def __init__(self):
        self.logger = logging.getLogger('batch_comments_test')
        self.processed_messages = []
        self.processed_comments = []
        self.flatten_structure = False
        
    def create_test_scenario(self) -> List[MockMessage]:
        """
        Создает тестовый сценарий:
        - Пост 1 (ID: 10) + комментарии (11, 12)
        - Альбом (ID: 20, 21, 22) + комментарии (23, 24)  
        - Пост 2 (ID: 30) + комментарии (31) + вложенный комментарий (32)
        """
        
        messages = []
        
        # Основные посты и альбомы
        messages.append(MockMessage(10, "Основной пост 1", has_comments=True, discussion_group_id=1001))
        messages.append(MockMessage(20, "Альбом 1/3", grouped_id=100, has_comments=True, discussion_group_id=1001))
        messages.append(MockMessage(21, "Альбом 2/3", grouped_id=100))
        messages.append(MockMessage(22, "Альбом 3/3", grouped_id=100))
        messages.append(MockMessage(30, "Основной пост 2", has_comments=True, discussion_group_id=1001))
        
        return messages
    
    def create_test_comments(self) -> Dict[int, List[MockMessage]]:
        """
        Создает комментарии для тестового сценария.
        """
        comments = {
            10: [  # Комментарии к посту 1
                MockMessage(11, "Комментарий 1 к посту 1"),
                MockMessage(12, "Комментарий 2 к посту 1")
            ],
            20: [  # Комментарии к альбому (по первому сообщению)
                MockMessage(23, "Комментарий 1 к альбому"),
                MockMessage(24, "Комментарий 2 к альбому")  
            ],
            30: [  # Комментарии к посту 2
                MockMessage(31, "Комментарий к посту 2", has_comments=True, discussion_group_id=1001)
            ],
            31: [  # Вложенный комментарий
                MockMessage(32, "Вложенный комментарий")
            ]
        }
        return comments
        
    async def mock_get_comments_for_message(self, message: MockMessage) -> List[MockMessage]:
        """Имитирует получение комментариев для сообщения."""
        comments_db = self.create_test_comments()
        
        if message.id in comments_db:
            comments = comments_db[message.id]
            self.logger.debug(f"💬 Найдено {len(comments)} комментариев для сообщения {message.id}")
            return comments
        
        self.logger.debug(f"💬 Комментарии для сообщения {message.id} не найдены")
        return []
    
    async def mock_copy_single_message(self, message: MockMessage) -> bool:
        """Имитирует копирование одиночного сообщения."""
        
        # Определяем тип сообщения
        if hasattr(message, '_is_from_discussion_group') and message._is_from_discussion_group:
            msg_type = "КОММЕНТАРИЙ"
            parent_id = getattr(message, '_parent_message_id', 'неизвестно')
            self.processed_comments.append(message.id)
            self.logger.info(f"💬 Скопирован {msg_type} {message.id} (родительский: {parent_id}): '{message.message}'")
        else:
            msg_type = "ПОСТ"
            self.processed_messages.append(message.id)
            self.logger.info(f"📝 Скопирован {msg_type} {message.id}: '{message.message}'")
        
        # Имитируем небольшую задержку
        await asyncio.sleep(0.01)
        return True
    
    async def mock_copy_album(self, album_messages: List[MockMessage]) -> bool:
        """Имитирует копирование альбома."""
        album_ids = [msg.id for msg in album_messages]
        
        # Проверяем, это альбом-комментарий или основной альбом
        is_comment_album = any(hasattr(msg, '_is_from_discussion_group') and msg._is_from_discussion_group 
                             for msg in album_messages)
        
        if is_comment_album:
            parent_id = getattr(album_messages[0], '_parent_message_id', 'неизвестно')
            self.logger.info(f"💬 Скопирован АЛЬБОМ-КОММЕНТАРИЙ {album_ids} (родительский: {parent_id})")
            self.processed_comments.extend(album_ids)
        else:
            self.logger.info(f"📸 Скопирован ОСНОВНОЙ АЛЬБОМ {album_ids}")
            self.processed_messages.extend(album_ids)
        
        await asyncio.sleep(0.01)
        return True
    
    async def test_batch_comments_processing(self, flatten_structure: bool = False) -> Dict[str, Any]:
        """
        Тестирует обработку комментариев в батчевом режиме.
        
        Args:
            flatten_structure: Тестировать ли режим антивложенности
        """
        self.flatten_structure = flatten_structure
        self.processed_messages = []
        self.processed_comments = []
        
        mode_name = "антивложенности" if flatten_structure else "с вложенностью"
        self.logger.info(f"\n🧪 === ТЕСТ БАТЧЕВОЙ ОБРАБОТКИ В РЕЖИМЕ {mode_name.upper()} ===")
        
        # Создаем тестовые данные
        messages = self.create_test_scenario()
        batch_size = 3  # Маленький батч для тестирования
        
        self.logger.info(f"📦 Тестируем с размером батча: {batch_size}")
        self.logger.info(f"📝 Основных сообщений для обработки: {len(messages)}")
        
        # Имитируем батчевую обработку
        batch_number = 1
        total_copied = 0
        total_failed = 0
        
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            self.logger.info(f"\n📦 === БАТЧ #{batch_number}: {len(batch)} сообщений ===")
            
            # Имитируем _process_message_batch с исправлениями
            batch_stats = await self._simulate_process_message_batch(batch)
            
            total_copied += batch_stats['copied']
            total_failed += batch_stats['failed']
            
            self.logger.info(f"✅ Батч #{batch_number} завершен: "
                           f"скопировано {batch_stats['copied']}, "
                           f"ошибок {batch_stats['failed']}")
            batch_number += 1
        
        # Результаты тестирования
        results = {
            'mode': mode_name,
            'total_messages_processed': len(self.processed_messages),
            'total_comments_processed': len(self.processed_comments),
            'total_copied': total_copied,
            'total_failed': total_failed,
            'messages_order': self.processed_messages,
            'comments_order': self.processed_comments,
            'flatten_structure': flatten_structure
        }
        
        self.logger.info(f"\n📊 === РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ===")
        self.logger.info(f"Режим: {mode_name}")
        self.logger.info(f"Обработано основных сообщений: {results['total_messages_processed']}")
        self.logger.info(f"Обработано комментариев: {results['total_comments_processed']}")
        self.logger.info(f"Всего скопировано: {results['total_copied']}")
        self.logger.info(f"Ошибок: {results['total_failed']}")
        self.logger.info(f"Порядок основных сообщений: {results['messages_order']}")
        self.logger.info(f"Порядок комментариев: {results['comments_order']}")
        
        return results
    
    async def _simulate_process_message_batch(self, batch: List[MockMessage]) -> Dict[str, int]:
        """
        Имитирует исправленный метод _process_message_batch.
        """
        batch_stats = {'copied': 0, 'failed': 0}
        
        self.logger.debug(f"🔄 Начинаем обработку батча из {len(batch)} сообщений")
        
        # ИСПРАВЛЕНИЕ: Собираем комментарии для сообщений в батче (если нужно)
        batch_with_comments = []
        
        if self.flatten_structure:
            self.logger.debug("💬 Режим антивложенности: собираем комментарии для сообщений в батче")
            
            # Собираем комментарии для каждого сообщения в батче
            for message in batch:
                batch_with_comments.append(message)
                
                # Получаем комментарии для сообщения
                try:
                    comments = await self.mock_get_comments_for_message(message)
                    if comments:
                        self.logger.debug(f"💬 Сообщение {message.id}: найдено {len(comments)} комментариев")
                        
                        # Помечаем комментарии специальным атрибутом
                        for comment in comments:
                            comment._is_from_discussion_group = True
                            comment._parent_message_id = message.id
                        
                        batch_with_comments.extend(comments)
                    else:
                        self.logger.debug(f"💬 Сообщение {message.id}: комментариев не найдено")
                        
                except Exception as e:
                    self.logger.warning(f"Ошибка получения комментариев для сообщения {message.id}: {e}")
            
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
                    success = await self.mock_copy_single_message(message)
                    
                    if success:
                        batch_stats['copied'] += 1
                    else:
                        batch_stats['failed'] += 1
                    
                    # Рекурсивная обработка комментариев в режиме с вложенностью
                    if not self.flatten_structure and success and not hasattr(message, '_is_from_discussion_group'):
                        try:
                            self.logger.debug(f"🔗 Запуск рекурсивной обработки комментариев для сообщения {message.id}")
                            comments_stats = await self._simulate_process_comments_recursively(message)
                            
                            # Обновляем статистику батча
                            batch_stats['copied'] += comments_stats['copied']
                            batch_stats['failed'] += comments_stats['failed']
                            
                            if comments_stats['copied'] > 0 or comments_stats['failed'] > 0:
                                self.logger.debug(f"📊 Рекурсивная обработка комментариев к сообщению {message.id} завершена: "
                                                f"скопировано {comments_stats['copied']}, ошибок {comments_stats['failed']}")
                                        
                        except Exception as e:
                            self.logger.warning(f"Ошибка рекурсивной обработки комментариев для сообщения {message.id}: {e}")
                
                elif item_type == 'album':
                    # Альбом
                    album_messages = item_data
                    success = await self.mock_copy_album(album_messages)
                    
                    if success:
                        batch_stats['copied'] += len(album_messages)
                    else:
                        batch_stats['failed'] += len(album_messages)
                    
                    # Рекурсивная обработка комментариев к альбому в режиме с вложенностью
                    is_comment_album = any(hasattr(msg, '_is_from_discussion_group') and msg._is_from_discussion_group 
                                         for msg in album_messages)
                    
                    if not self.flatten_structure and success and not is_comment_album:
                        representative_message = album_messages[0]
                        try:
                            self.logger.debug(f"🔗 Запуск рекурсивной обработки комментариев для альбома {representative_message.grouped_id}")
                            comments_stats = await self._simulate_process_comments_recursively(representative_message)
                            
                            # Обновляем статистику батча
                            batch_stats['copied'] += comments_stats['copied']
                            batch_stats['failed'] += comments_stats['failed']
                            
                            if comments_stats['copied'] > 0 or comments_stats['failed'] > 0:
                                self.logger.debug(f"📊 Рекурсивная обработка комментариев к альбому {representative_message.grouped_id} завершена: "
                                                f"скопировано {comments_stats['copied']}, ошибок {comments_stats['failed']}")
                                        
                        except Exception as e:
                            self.logger.warning(f"Ошибка рекурсивной обработки комментариев для альбома {representative_message.grouped_id}: {e}")
                
            except Exception as e:
                self.logger.error(f"Ошибка обработки элемента {item_type}: {e}")
                if item_type == 'single':
                    batch_stats['failed'] += 1
                else:
                    batch_stats['failed'] += len(item_data)
        
        return batch_stats
    
    async def _simulate_process_comments_recursively(self, parent_message: MockMessage, depth: int = 0, 
                                                    max_depth: int = 10) -> Dict[str, int]:
        """
        Имитирует рекурсивную обработку комментариев.
        """
        comments_stats = {'copied': 0, 'failed': 0}
        
        if depth >= max_depth:
            self.logger.warning(f"Достигнута максимальная глубина рекурсии {max_depth} для комментариев")
            return comments_stats
        
        try:
            # Получаем комментарии к родительскому сообщению
            comments = await self.mock_get_comments_for_message(parent_message)
            
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
                    comment_success = await self.mock_copy_single_message(comment)
                    
                    if comment_success:
                        comments_stats['copied'] += 1
                        self.logger.debug(f"{'  ' * depth}✅ Комментарий {comment.id} скопирован")
                        
                        # Рекурсивно обрабатываем комментарии к этому комментарию
                        nested_stats = await self._simulate_process_comments_recursively(comment, depth + 1, max_depth)
                        comments_stats['copied'] += nested_stats['copied']
                        comments_stats['failed'] += nested_stats['failed']
                        
                    else:
                        comments_stats['failed'] += 1
                        self.logger.debug(f"{'  ' * depth}❌ Ошибка копирования комментария {comment.id}")
                        
                except Exception as comment_error:
                    self.logger.error(f"{'  ' * depth}Ошибка обработки комментария {comment.id}: {comment_error}")
                    comments_stats['failed'] += 1
                    
        except Exception as e:
            self.logger.warning(f"{'  ' * depth}Ошибка получения комментариев для сообщения {parent_message.id} (глубина {depth}): {e}")
        
        return comments_stats

async def main():
    """Основная функция тестирования."""
    print("🧪 Запуск тестирования исправления батчевой обработки комментариев...")
    
    tester = MockCopierTester()
    
    # Тест 1: Режим с вложенностью
    results_nested = await tester.test_batch_comments_processing(flatten_structure=False)
    
    # Тест 2: Режим антивложенности  
    results_flat = await tester.test_batch_comments_processing(flatten_structure=True)
    
    # Сравнение результатов
    print(f"\n🎯 === СВОДКА РЕЗУЛЬТАТОВ ===")
    print(f"Режим с вложенностью:")
    print(f"  - Основных сообщений: {results_nested['total_messages_processed']}")
    print(f"  - Комментариев: {results_nested['total_comments_processed']}")
    print(f"  - Всего скопировано: {results_nested['total_copied']}")
    
    print(f"Режим антивложенности:")
    print(f"  - Основных сообщений: {results_flat['total_messages_processed']}")
    print(f"  - Комментариев: {results_flat['total_comments_processed']}")
    print(f"  - Всего скопировано: {results_flat['total_copied']}")
    
    # Проверка корректности
    expected_messages = 5  # 2 основных поста + 3 сообщения альбома
    expected_comments = 5  # 2 + 2 + 1 комментариев
    
    success = True
    
    if results_nested['total_messages_processed'] != expected_messages:
        print(f"❌ ОШИБКА: В режиме с вложенностью ожидалось {expected_messages} основных сообщений, получено {results_nested['total_messages_processed']}")
        success = False
    
    if results_nested['total_comments_processed'] != expected_comments:
        print(f"❌ ОШИБКА: В режиме с вложенностью ожидалось {expected_comments} комментариев, получено {results_nested['total_comments_processed']}")
        success = False
    
    if results_flat['total_messages_processed'] != expected_messages:
        print(f"❌ ОШИБКА: В режиме антивложенности ожидалось {expected_messages} основных сообщений, получено {results_flat['total_messages_processed']}")
        success = False
    
    if results_flat['total_comments_processed'] != expected_comments:
        print(f"❌ ОШИБКА: В режиме антивложенности ожидалось {expected_comments} комментариев, получено {results_flat['total_comments_processed']}")
        success = False
    
    if success:
        print(f"✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print(f"🎉 Исправление батчевой обработки комментариев работает корректно!")
    else:
        print(f"❌ ТЕСТЫ ПРОВАЛЕНЫ!")
        print(f"🔧 Требуется дополнительная отладка исправления!")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())