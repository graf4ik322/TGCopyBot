"""
Модуль для загрузки конфигурации приложения.
Поддерживает загрузку из .env файлов и переменных окружения.
"""

import os
from typing import Optional
from dotenv import load_dotenv


class Config:
    """Класс для управления конфигурацией приложения."""
    
    def __init__(self, env_file: str = '.env'):
        """
        Инициализация конфигурации.
        
        Args:
            env_file: Путь к файлу с переменными окружения
        """
        load_dotenv(env_file)
        
        # Telegram API credentials
        self.api_id: int = int(os.getenv('API_ID', '0'))
        self.api_hash: str = os.getenv('API_HASH', '')
        self.phone: str = os.getenv('PHONE', '')
        
        # Groups configuration
        source_id_str = os.getenv('SOURCE_GROUP_ID', '')
        target_id_str = os.getenv('TARGET_GROUP_ID', '')
        
        # ИСПРАВЛЕНО: Автоматическое преобразование числовых ID в int
        self.source_group_id = self._parse_entity_id(source_id_str)
        self.target_group_id = self._parse_entity_id(target_id_str)
        
        # Behavior settings
        self.delay_seconds: int = int(os.getenv('DELAY_SECONDS', '3'))
        self.messages_per_hour: int = int(os.getenv('MESSAGES_PER_HOUR', '30'))
        self.dry_run: bool = os.getenv('DRY_RUN', 'false').lower() == 'true'
        
        # Session and storage
        self.session_name: str = os.getenv('SESSION_NAME', 'telegram_copier')
        self.resume_file: str = os.getenv('RESUME_FILE', 'last_message_id.txt')

        # Настройки трекинга сообщений
        self.use_message_tracker: bool = os.getenv("USE_MESSAGE_TRACKER", "true").lower() == "true"
        self.tracker_file: str = os.getenv("TRACKER_FILE", "copied_messages.json")
        self.add_debug_tags: bool = os.getenv("ADD_DEBUG_TAGS", "false").lower() == "true"
        
        # НОВОЕ: Отладочный режим - добавление ID сообщений к тексту
        self.debug_message_ids: bool = os.getenv("DEBUG_MESSAGE_IDS", "false").lower() == "true"
        
        # НОВОЕ: Настройка антивложенности
        self.flatten_structure = os.getenv('FLATTEN_STRUCTURE', 'false').lower() == 'true'
        
        # ИСПРАВЛЕНИЕ: Размер батча для предотвращения проблем с памятью
        self.batch_size: int = int(os.getenv('BATCH_SIZE', '100'))
        
        # НОВОЕ: Настройки оптимизации памяти
        self.enable_memory_optimization: bool = os.getenv('ENABLE_MEMORY_OPTIMIZATION', 'true').lower() == 'true'
        self.memory_limit_mb: int = int(os.getenv('MEMORY_LIMIT_MB', '100'))
        self.lru_cache_size: int = int(os.getenv('LRU_CACHE_SIZE', '1000'))
        
        # Proxy settings (optional)
        self.proxy_server: Optional[str] = os.getenv('PROXY_SERVER')
        # ИСПРАВЛЕНИЕ: Безопасное преобразование PROXY_PORT
        proxy_port_str = os.getenv('PROXY_PORT')
        if proxy_port_str and proxy_port_str.isdigit():
            self.proxy_port: Optional[int] = int(proxy_port_str)
        else:
            self.proxy_port: Optional[int] = None
        self.proxy_username: Optional[str] = os.getenv('PROXY_USERNAME')
        self.proxy_password: Optional[str] = os.getenv('PROXY_PASSWORD')
    
    def validate(self) -> bool:
        """
        Валидация конфигурации.
        
        Returns:
            True если конфигурация валидна, False иначе
        """
        required_fields = [
            ('API_ID', self.api_id),
            ('API_HASH', self.api_hash),
            ('PHONE', self.phone),
            ('SOURCE_GROUP_ID', self.source_group_id),
            ('TARGET_GROUP_ID', self.target_group_id)
        ]
        
        for field_name, field_value in required_fields:
            if not field_value:
                print(f"Ошибка: отсутствует обязательное поле {field_name}")
                return False
        
        if self.api_id == 0:
            print("Ошибка: API_ID должен быть числом больше 0")
            return False
        
        return True
    
    def _parse_entity_id(self, entity_str: str):
        """
        НОВОЕ: Парсинг entity ID с автоматическим определением типа.
        
        Args:
            entity_str: Строка с ID канала (числовой или username)
            
        Returns:
            int для числовых ID, str для username
        """
        if not entity_str:
            return ''
            
        entity_str = entity_str.strip()
        
        # Если начинается с @, то это username
        if entity_str.startswith('@'):
            return entity_str
            
        # Если начинается с -100, то это числовой ID суперчата
        if entity_str.startswith('-100'):
            try:
                return int(entity_str)
            except ValueError:
                return entity_str
                
        # Если это просто число (положительное или отрицательное)
        if entity_str.lstrip('-').isdigit():
            try:
                return int(entity_str)
            except ValueError:
                return entity_str
                
        # Иначе считаем username без @
        return entity_str
    
    def get_proxy_config(self) -> Optional[dict]:
        """
        Получить конфигурацию прокси для Telethon.
        
        Returns:
            Словарь с настройками прокси или None
        """
        if not self.proxy_server:
            return None
        
        proxy_config = {
            'proxy_type': 'socks5',  # По умолчанию SOCKS5
            'addr': self.proxy_server,
            'port': self.proxy_port or 1080
        }
        
        if self.proxy_username:
            proxy_config['username'] = self.proxy_username
        
        if self.proxy_password:
            proxy_config['password'] = self.proxy_password
        
        return proxy_config