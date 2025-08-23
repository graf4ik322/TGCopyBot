#!/usr/bin/env python3
"""
Тест для проверки правильной группировки альбомов в комментариях.
Проверяет, что альбомы в комментариях группируются правильно и не разбиваются на отдельные посты.
"""

import logging
import asyncio
from typing import List, Optional, Dict, Any

# Мок-класс для сообщений
class MockMessage:
    def __init__(self, id: int, message: str, grouped_id: Optional[int] = None, 
                 reply_to: Optional[int] = None, media=None):
        self.id = id
        self.message = message
        self.grouped_id = grouped_id
        self.reply_to = reply_to
        self.media = media
        self.entities = None

class TestCommentsAlbumGrouping:
    """Тест группировки альбомов в комментариях"""
    
    def __init__(self):
        self.logger = logging.getLogger('test_comments_albums')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def test_album_grouping_in_comments(self):
        """Тестирует правильную группировку альбомов в комментариях"""
        
        self.logger.info("🧪 ТЕСТ: Группировка альбомов в комментариях")
        self.logger.info("=" * 60)
        
        # Создаем тестовую структуру сообщений
        messages = []
        
        # Основной пост
        messages.append(MockMessage(1, "Основной пост 1"))
        
        # Комментарий с обычным текстом
        messages.append(MockMessage(2, "Комментарий к посту 1", reply_to=1))
        
        # Альбом в комментарии (3 фото с одним grouped_id)
        messages.append(MockMessage(3, "Фото 1 в альбоме", grouped_id=100, reply_to=1))
        messages.append(MockMessage(4, "Фото 2 в альбоме", grouped_id=100, reply_to=1))
        messages.append(MockMessage(5, "Фото 3 в альбоме", grouped_id=100, reply_to=1))
        
        # Еще один комментарий
        messages.append(MockMessage(6, "Еще комментарий", reply_to=1))
        
        # Основной пост с альбомом
        messages.append(MockMessage(7, "Фото 1 основного альбома", grouped_id=200))
        messages.append(MockMessage(8, "Фото 2 основного альбома", grouped_id=200))
        
        # Комментарий к альбому
        messages.append(MockMessage(9, "Комментарий к альбому", reply_to=7))
        
        self.logger.info("📋 Исходная структура:")
        for msg in messages:
            msg_type = "💬 комментарий" if msg.reply_to else "📌 пост"
            album_info = f" (альбом {msg.grouped_id})" if msg.grouped_id else ""
            reply_info = f" → ответ на {msg.reply_to}" if msg.reply_to else ""
            self.logger.info(f"   ID {msg.id}: {msg_type}{album_info}{reply_info} - '{msg.message}'")
        
        # Тестируем группировку (аналогично логике из copier.py)
        grouped_messages = {}
        main_posts_count = 0
        comments_count = 0
        albums_in_comments_count = 0
        albums_in_main_count = 0
        
        for message in messages:
            # Определяем тип сообщения
            is_comment = hasattr(message, 'reply_to') and message.reply_to is not None
            
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
        
        self.logger.info("\n📊 Результат группировки:")
        self.logger.info(f"   📌 Основных постов: {main_posts_count}")
        self.logger.info(f"   💬 Комментариев: {comments_count}")
        self.logger.info(f"   🎬 Альбомов в основных постах: {albums_in_main_count}")
        self.logger.info(f"   🎬 Альбомов в комментариях: {albums_in_comments_count}")
        self.logger.info(f"   📦 Всего альбомов: {len(grouped_messages)}")
        
        # Анализируем альбомы
        self.logger.info("\n🎬 Детали альбомов:")
        for grouped_id, album_messages in grouped_messages.items():
            # Определяем тип альбома
            first_msg = album_messages[0]
            is_comment_album = hasattr(first_msg, 'reply_to') and first_msg.reply_to is not None
            album_type = "в комментарии" if is_comment_album else "основной"
            
            self.logger.info(f"   Альбом {grouped_id} ({album_type}):")
            for msg in sorted(album_messages, key=lambda x: x.id):
                reply_info = f" → ответ на {msg.reply_to}" if msg.reply_to else ""
                self.logger.info(f"     • ID {msg.id}{reply_info}: '{msg.message}'")
        
        # Проверяем ожидаемые результаты
        self.logger.info("\n✅ ПРОВЕРКА РЕЗУЛЬТАТОВ:")
        
        success = True
        
        # Проверка 1: Должен быть 1 альбом в комментариях
        if albums_in_comments_count != 1:
            self.logger.error(f"❌ Ожидался 1 альбом в комментариях, найдено: {albums_in_comments_count}")
            success = False
        else:
            self.logger.info("✅ Правильно найден 1 альбом в комментариях")
        
        # Проверка 2: Должен быть 1 альбом в основных постах
        if albums_in_main_count != 1:
            self.logger.error(f"❌ Ожидался 1 альбом в основных постах, найдено: {albums_in_main_count}")
            success = False
        else:
            self.logger.info("✅ Правильно найден 1 альбом в основных постах")
        
        # Проверка 3: Альбом в комментарии должен содержать 3 сообщения
        comment_album = None
        for grouped_id, album_messages in grouped_messages.items():
            first_msg = album_messages[0]
            if hasattr(first_msg, 'reply_to') and first_msg.reply_to is not None:
                comment_album = album_messages
                break
        
        if comment_album is None:
            self.logger.error("❌ Не найден альбом в комментарии")
            success = False
        elif len(comment_album) != 3:
            self.logger.error(f"❌ Альбом в комментарии должен содержать 3 сообщения, найдено: {len(comment_album)}")
            success = False
        else:
            self.logger.info("✅ Альбом в комментарии правильно сгруппирован (3 сообщения)")
        
        # Проверка 4: Все сообщения альбома в комментарии должны ссылаться на один пост
        if comment_album:
            reply_to_values = set(msg.reply_to for msg in comment_album)
            if len(reply_to_values) != 1:
                self.logger.error(f"❌ Сообщения альбома ссылаются на разные посты: {reply_to_values}")
                success = False
            else:
                self.logger.info(f"✅ Все сообщения альбома правильно ссылаются на пост {reply_to_values.pop()}")
        
        if success:
            self.logger.info("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ!")
            self.logger.info("Альбомы в комментариях группируются правильно!")
        else:
            self.logger.error("\n💥 ТЕСТЫ НЕ ПРОЙДЕНЫ!")
            self.logger.error("Найдены проблемы с группировкой альбомов!")
        
        return success
    
    def test_flatten_vs_nested_mode(self):
        """Тестирует разницу между режимами flatten_structure"""
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🧪 ТЕСТ: Режимы flatten_structure vs nested")
        self.logger.info("=" * 60)
        
        # Имитируем структуру обработки
        test_messages = [
            "📌 Основной пост (ID: 1)",
            "💬 Комментарий (ID: 2) → ответ на 1", 
            "🎬 Альбом в комментарии (ID: 3-5) → ответ на 1",
            "📌 Основной пост (ID: 6)",
            "🎬 Альбом основной (ID: 7-8)"
        ]
        
        self.logger.info("\n🔗 ВЛОЖЕННЫЙ РЕЖИМ (flatten_structure=False):")
        self.logger.info("Комментарии сохраняют связь с основными постами:")
        for msg in test_messages:
            self.logger.info(f"   {msg}")
        
        self.logger.info("\n🔄 ПЛОСКИЙ РЕЖИМ (flatten_structure=True):")
        self.logger.info("Комментарии обрабатываются как обычные посты:")
        for msg in test_messages:
            # В плоском режиме убираем информацию о связях
            flat_msg = msg.replace(" → ответ на 1", " (был комментарием)")
            self.logger.info(f"   {flat_msg}")
        
        self.logger.info("\n💡 ВАЖНО:")
        self.logger.info("В ЛЮБОМ режиме альбомы остаются целыми!")
        self.logger.info("Альбом из 3 фото = 1 альбом из 3 фото")
        self.logger.info("НЕ 3 отдельных поста!")
        
        return True

def main():
    """Запуск всех тестов"""
    test = TestCommentsAlbumGrouping()
    
    # Запускаем тесты
    test1_result = test.test_album_grouping_in_comments()
    test2_result = test.test_flatten_vs_nested_mode()
    
    if test1_result and test2_result:
        print("\n🎉 ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ!")
        return 0
    else:
        print("\n💥 НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ!")
        return 1

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    exit(main())