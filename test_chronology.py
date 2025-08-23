#!/usr/bin/env python3
"""
Тестовый скрипт для демонстрации новой хронологической логики копирования.
Показывает, как сообщения теперь обрабатываются в строгом порядке.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
from unittest.mock import Mock, AsyncMock

# Настройка логирования для теста
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MockMessage:
    """Мок объект для имитации сообщения Telegram."""
    
    def __init__(self, message_id: int, text: str = "", grouped_id: Optional[int] = None, 
                 reply_to: Optional[int] = None, media=None):
        self.id = message_id
        self.message = text
        self.grouped_id = grouped_id
        self.reply_to = reply_to
        self.media = media
        self.entities = None

class ChronologyTester:
    """Тестер для демонстрации хронологической логики."""
    
    def __init__(self):
        self.logger = logging.getLogger('chronology_test')
        self.processed_order = []  # Записываем порядок обработки
        
    async def simulate_message_processing(self):
        """Симуляция обработки сообщений с новой логикой."""
        
        # Создаем тестовые сообщения в хронологическом порядке
        messages = self._create_test_messages()
        
        self.logger.info("🔄 Начинаем тестирование хронологической обработки")
        self.logger.info(f"📋 Подготовлено {len(messages)} тестовых сообщений")
        
        # Симулируем обработку в хронологическом порядке
        pending_albums = {}
        
        for message in messages:
            self.logger.info(f"\n📨 Обрабатываем сообщение {message.id}: '{message.message[:30]}'")
            
            # Симулируем обработку основного сообщения
            success = await self._process_message_chronologically(
                message, pending_albums
            )
            
            if success:
                # Симулируем обработку комментариев
                await self._process_message_comments(message)
                
                self.processed_order.append(f"MSG_{message.id}")
                self.logger.info(f"✅ Сообщение {message.id} полностью обработано")
        
        # Завершаем оставшиеся альбомы
        await self._finalize_pending_albums(pending_albums)
        
        self._show_results()
    
    def _create_test_messages(self) -> List[MockMessage]:
        """Создает тестовые сообщения для демонстрации."""
        messages = []
        
        # Пост 1 (одиночное сообщение)
        messages.append(MockMessage(1, "Первый пост - одиночное сообщение"))
        
        # Комментарии к посту 1
        messages.append(MockMessage(2, "Комментарий 1 к посту 1", reply_to=1))
        messages.append(MockMessage(3, "Комментарий 2 к посту 1", reply_to=1))
        
        # Альбом (пост 2) - сообщения с одинаковым grouped_id
        messages.append(MockMessage(4, "Пост 2 - альбом, фото 1", grouped_id=100))
        messages.append(MockMessage(5, "Пост 2 - альбом, фото 2", grouped_id=100))
        messages.append(MockMessage(6, "Пост 2 - альбом, фото 3", grouped_id=100))
        
        # Комментарии к альбому
        messages.append(MockMessage(7, "Комментарий к альбому", reply_to=4))
        messages.append(MockMessage(8, "Альбом в комментарии 1", grouped_id=200, reply_to=4))
        messages.append(MockMessage(9, "Альбом в комментарии 2", grouped_id=200, reply_to=4))
        
        # Пост 3 (одиночное сообщение)
        messages.append(MockMessage(10, "Третий пост - текстовое сообщение"))
        
        # Комментарий к посту 3
        messages.append(MockMessage(11, "Комментарий к посту 3", reply_to=10))
        
        # Вложенный комментарий (комментарий к комментарию)
        messages.append(MockMessage(12, "Ответ на комментарий", reply_to=11))
        
        self.logger.info("📝 Создана тестовая структура:")
        self.logger.info("   📌 Пост 1 (ID: 1) + 2 комментария (ID: 2, 3)")
        self.logger.info("   🎬 Альбом (ID: 4-6) + комментарий (ID: 7) + альбом в комментарии (ID: 8-9)")
        self.logger.info("   📌 Пост 3 (ID: 10) + комментарий (ID: 11) + вложенный комментарий (ID: 12)")
        
        return messages
    
    async def _process_message_chronologically(self, message: MockMessage, 
                                             pending_albums: Dict[int, List[MockMessage]]) -> bool:
        """Симуляция хронологической обработки сообщения."""
        
        if message.grouped_id:
            return await self._handle_album_message(message, pending_albums)
        else:
            return await self._process_single_message(message)
    
    async def _handle_album_message(self, message: MockMessage, 
                                   pending_albums: Dict[int, List[MockMessage]]) -> bool:
        """Симуляция обработки сообщения альбома."""
        
        grouped_id = message.grouped_id
        
        if grouped_id not in pending_albums:
            pending_albums[grouped_id] = []
        pending_albums[grouped_id].append(message)
        
        self.logger.info(f"   📎 Добавлено в альбом {grouped_id} (теперь {len(pending_albums[grouped_id])} сообщений)")
        
        # Симулируем проверку завершенности альбома
        # В реальности мы бы проверяли следующее сообщение
        next_message_id = message.id + 1
        next_in_album = any(
            msg.id == next_message_id and msg.grouped_id == grouped_id 
            for msg in self._get_all_test_messages() 
            if msg.id > message.id
        )
        
        if not next_in_album:
            # Альбом завершен
            album_messages = pending_albums.pop(grouped_id)
            album_messages.sort(key=lambda x: x.id)
            
            self.logger.info(f"   🎬 Альбом {grouped_id} завершен - обрабатываем {len(album_messages)} сообщений")
            
            # Симулируем копирование альбома
            await asyncio.sleep(0.1)  # Имитация сетевой задержки
            
            # Записываем порядок обработки
            album_ids = [str(msg.id) for msg in album_messages]
            self.processed_order.append(f"ALBUM_{grouped_id}({','.join(album_ids)})")
            
            return True
        else:
            # Альбом еще не завершен
            self.logger.info(f"   ⏳ Альбом {grouped_id} еще не завершен")
            return False
    
    async def _process_single_message(self, message: MockMessage) -> bool:
        """Симуляция обработки одиночного сообщения."""
        
        self.logger.info(f"   💬 Обрабатываем одиночное сообщение")
        
        # Симулируем копирование сообщения
        await asyncio.sleep(0.1)  # Имитация сетевой задержки
        
        return True
    
    async def _process_message_comments(self, parent_message: MockMessage) -> None:
        """Симуляция обработки комментариев к сообщению."""
        
        # Находим все комментарии к этому сообщению
        comments = [
            msg for msg in self._get_all_test_messages() 
            if msg.reply_to == parent_message.id
        ]
        
        if comments:
            self.logger.info(f"   📝 Найдено {len(comments)} комментариев к сообщению {parent_message.id}")
            
            # Обрабатываем комментарии в хронологическом порядке
            pending_comment_albums = {}
            
            for comment in sorted(comments, key=lambda x: x.id):
                self.logger.info(f"     💬 Обрабатываем комментарий {comment.id}: '{comment.message[:20]}'")
                
                success = await self._process_message_chronologically(
                    comment, pending_comment_albums
                )
                
                if success:
                    # Рекурсивно обрабатываем комментарии к комментарию
                    await self._process_message_comments(comment)
                    self.processed_order.append(f"COMMENT_{comment.id}")
            
            # Завершаем альбомы в комментариях
            await self._finalize_pending_albums(pending_comment_albums)
    
    async def _finalize_pending_albums(self, pending_albums: Dict[int, List[MockMessage]]) -> None:
        """Завершение обработки оставшихся альбомов."""
        
        for grouped_id, album_messages in pending_albums.items():
            if album_messages:
                album_messages.sort(key=lambda x: x.id)
                self.logger.info(f"🎬 Завершаем альбом {grouped_id} ({len(album_messages)} сообщений)")
                
                await asyncio.sleep(0.1)  # Имитация обработки
                
                album_ids = [str(msg.id) for msg in album_messages]
                self.processed_order.append(f"FINAL_ALBUM_{grouped_id}({','.join(album_ids)})")
        
        pending_albums.clear()
    
    def _get_all_test_messages(self) -> List[MockMessage]:
        """Возвращает все тестовые сообщения (для симуляции)."""
        return self._create_test_messages()
    
    def _show_results(self):
        """Показывает результаты тестирования."""
        
        self.logger.info("\n" + "="*60)
        self.logger.info("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ХРОНОЛОГИИ")
        self.logger.info("="*60)
        
        self.logger.info("\n🔄 Порядок обработки:")
        for i, item in enumerate(self.processed_order, 1):
            self.logger.info(f"  {i:2d}. {item}")
        
        self.logger.info("\n✅ КЛЮЧЕВЫЕ УЛУЧШЕНИЯ:")
        self.logger.info("   1. Строгая хронология: сообщения обрабатываются по порядку")
        self.logger.info("   2. Поддержка вложенности: комментарии обрабатываются сразу после родительского сообщения")
        self.logger.info("   3. Сохранена логика альбомов: группированные медиа остаются целыми")
        self.logger.info("   4. Рекурсивная обработка: комментарии к комментариям тоже учитываются")
        
        self.logger.info("\n🎯 ОЖИДАЕМЫЙ ПОРЯДОК В ЦЕЛЕВОМ КАНАЛЕ:")
        expected_order = [
            "Пост 1 (сообщение 1)",
            "├─ Комментарий 1 (сообщение 2)",
            "├─ Комментарий 2 (сообщение 3)",
            "Альбом (сообщения 4-6)",
            "├─ Комментарий к альбому (сообщение 7)",
            "├─ Альбом в комментарии (сообщения 8-9)",
            "Пост 3 (сообщение 10)",
            "├─ Комментарий к посту 3 (сообщение 11)",
            "    └─ Ответ на комментарий (сообщение 12)"
        ]
        
        for order in expected_order:
            self.logger.info(f"   {order}")
        
        self.logger.info("\n" + "="*60)

async def main():
    """Запуск тестирования."""
    tester = ChronologyTester()
    await tester.simulate_message_processing()

if __name__ == "__main__":
    asyncio.run(main())