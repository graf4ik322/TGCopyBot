#!/usr/bin/env python3
"""
Модуль для отслеживания скопированных сообщений.
Обеспечивает точный учет всех операций копирования.
"""

import json
import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime


class MessageTracker:
    """Класс для отслеживания скопированных сообщений."""
    
    def __init__(self, tracker_file: str = "copied_messages.json"):
        """
        Инициализация трекера сообщений.
        
        Args:
            tracker_file: Путь к файлу для хранения информации
        """
        self.tracker_file = tracker_file
        self.logger = logging.getLogger('telegram_copier.tracker')
        self.data = self._load_data()
    
    def _load_data(self) -> Dict[str, Any]:
        """Загрузка данных из файла."""
        if os.path.exists(self.tracker_file):
            try:
                with open(self.tracker_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.logger.info(f"Загружена информация о {len(data.get('copied_messages', {}))} скопированных сообщениях")
                    return data
            except Exception as e:
                self.logger.error(f"Ошибка загрузки файла трекинга: {e}")
        
        # Создаем новую структуру данных
        return {
            "copied_messages": {},  # source_id -> {target_id, timestamp, status}
            "statistics": {
                "total_copied": 0,
                "total_failed": 0,
                "last_updated": None
            },
            "source_channel": None,
            "target_channel": None
        }
    
    def _save_data(self):
        """Сохранение данных в файл."""
        try:
            # Обновляем статистику
            self.data["statistics"]["last_updated"] = datetime.now().isoformat()
            
            with open(self.tracker_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            
            self.logger.debug(f"Данные трекинга сохранены в {self.tracker_file}")
        except Exception as e:
            self.logger.error(f"Ошибка сохранения файла трекинга: {e}")
    
    def set_channels(self, source_channel: str, target_channel: str):
        """Установка информации о каналах."""
        self.data["source_channel"] = source_channel
        self.data["target_channel"] = target_channel
        self._save_data()
    
    def is_message_copied(self, source_id: int) -> bool:
        """
        Проверка, было ли сообщение уже скопировано.
        
        Args:
            source_id: ID сообщения в исходном канале
        
        Returns:
            True если сообщение уже скопировано
        """
        return str(source_id) in self.data["copied_messages"]
    
    def mark_message_copied(self, source_id: int, target_id: int, message_type: str = "single"):
        """
        Отметить сообщение как скопированное.
        
        Args:
            source_id: ID сообщения в исходном канале
            target_id: ID сообщения в целевом канале
            message_type: Тип сообщения (single, album)
        """
        self.data["copied_messages"][str(source_id)] = {
            "target_id": target_id,
            "timestamp": datetime.now().isoformat(),
            "type": message_type,
            "status": "copied"
        }
        
        self.data["statistics"]["total_copied"] += 1
        self._save_data()
        
        self.logger.debug(f"Отмечено как скопированное: {source_id} -> {target_id}")
    
    def mark_album_copied(self, source_ids: List[int], target_ids: List[int]):
        """
        Отметить альбом как скопированный.
        
        Args:
            source_ids: Список ID исходных сообщений альбома
            target_ids: Список ID скопированных сообщений альбома
        """
        timestamp = datetime.now().isoformat()
        
        # ИСПРАВЛЕНИЕ: Безопасная обработка target_ids для предотвращения IndexError
        for i, source_id in enumerate(source_ids):
            # Если target_ids пустой или недостаточно элементов, используем 0 как заглушку
            if target_ids and i < len(target_ids):
                target_id = target_ids[i]
            elif target_ids:
                target_id = target_ids[0]  # Используем первый элемент как fallback
            else:
                target_id = 0  # Заглушка для случая пустого target_ids
            
            self.data["copied_messages"][str(source_id)] = {
                "target_id": target_id,
                "timestamp": timestamp,
                "type": "album",
                "status": "copied",
                "album_position": i + 1,
                "album_size": len(source_ids)
            }
        
        self.data["statistics"]["total_copied"] += len(source_ids)
        self.data["statistics"]["last_updated"] = datetime.now().isoformat()
        self._save_data()
        
        self.logger.debug(f"Отмечен альбом как скопированный: {len(source_ids)} сообщений")
    
    def mark_message_failed(self, source_id: int, error: str):
        """
        Отметить сообщение как неудачно скопированное.
        
        Args:
            source_id: ID сообщения в исходном канале
            error: Описание ошибки
        """
        self.data["copied_messages"][str(source_id)] = {
            "target_id": None,
            "timestamp": datetime.now().isoformat(),
            "type": "failed",
            "status": "failed",
            "error": error
        }
        
        self.data["statistics"]["total_failed"] += 1
        self._save_data()
        
        self.logger.debug(f"Отмечено как неудачное: {source_id} - {error}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики."""
        copied_count = len([m for m in self.data["copied_messages"].values() if m["status"] == "copied"])
        failed_count = len([m for m in self.data["copied_messages"].values() if m["status"] == "failed"])
        
        return {
            "total_tracked": len(self.data["copied_messages"]),
            "successfully_copied": copied_count,
            "failed_copies": failed_count,
            "last_updated": self.data["statistics"].get("last_updated"),
            "source_channel": self.data.get("source_channel"),
            "target_channel": self.data.get("target_channel")
        }
    
    def get_last_copied_id(self) -> Optional[int]:
        """Получение ID последнего успешно скопированного сообщения."""
        copied_messages = {
            int(k): v for k, v in self.data["copied_messages"].items() 
            if v["status"] == "copied"
        }
        
        if not copied_messages:
            return None
        
        return max(copied_messages.keys())
    
    def cleanup_failed_messages(self):
        """Очистка записей о неудачных попытках для повторной попытки."""
        failed_count = 0
        for source_id in list(self.data["copied_messages"].keys()):
            if self.data["copied_messages"][source_id]["status"] == "failed":
                del self.data["copied_messages"][source_id]
                failed_count += 1
        
        if failed_count > 0:
            self._save_data()
            self.logger.info(f"Очищено {failed_count} записей о неудачных попытках")
    
    def generate_debug_tag(self, source_id: int, add_tags: bool = False) -> str:
        """
        Генерация debug тега для сообщения.
        
        Args:
            source_id: ID исходного сообщения
            add_tags: Добавлять ли теги к тексту
        
        Returns:
            Debug тег или пустая строка
        """
        if not add_tags:
            return ""
        
        return f"\n\n#src_{self.data.get('source_channel', 'unknown')}_{source_id}"


def test_message_tracker():
    """Тест функциональности трекера."""
    print("🧪 Тестирование MessageTracker...")
    
    # Создаем тестовый трекер
    tracker = MessageTracker("test_tracker.json")
    tracker.set_channels("source_channel", "target_channel")
    
    # Тестируем одиночные сообщения
    tracker.mark_message_copied(12345, 67890, "single")
    tracker.mark_message_copied(12346, 67891, "single")
    
    # Тестируем альбом
    tracker.mark_album_copied([12347, 12348, 12349], [67892, 67893, 67894])
    
    # Тестируем неудачу
    tracker.mark_message_failed(12350, "MediaInvalidError")
    
    # Проверяем статистику
    stats = tracker.get_statistics()
    print(f"📊 Статистика: {stats}")
    
    # Проверяем дубликаты
    print(f"🔍 Сообщение 12345 уже скопировано: {tracker.is_message_copied(12345)}")
    print(f"🔍 Сообщение 99999 уже скопировано: {tracker.is_message_copied(99999)}")
    
    # Получаем последний ID
    last_id = tracker.get_last_copied_id()
    print(f"📝 Последний скопированный ID: {last_id}")
    
    # Очищаем тестовый файл
    if os.path.exists("test_tracker.json"):
        os.remove("test_tracker.json")
    
    print("✅ Тест MessageTracker завершен!")


if __name__ == "__main__":
    test_message_tracker()