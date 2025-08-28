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
        self.source_group_id: str = os.getenv('SOURCE_GROUP_ID', '')
        self.target_group_id: str = os.getenv('TARGET_GROUP_ID', '')
        
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
        
        # НОВОЕ: Настройка антивложенности
        self.flatten_structure = os.getenv('FLATTEN_STRUCTURE', 'false').lower() == 'true'
        
        # Logging settings
        self.log_level: str = os.getenv('LOG_LEVEL', 'INFO').upper()
        
        # Message deletion settings
        self.deletion_batch_size: int = int(os.getenv('DELETION_BATCH_SIZE', '100'))
        self.deletion_messages_per_hour: int = int(os.getenv('DELETION_MESSAGES_PER_HOUR', '6000'))
        self.deletion_delay_seconds: int = int(os.getenv('DELETION_DELAY_SECONDS', '1'))
        self.deletion_timeout_seconds: int = int(os.getenv('DELETION_TIMEOUT_SECONDS', '30'))
        self.deletion_max_range_warning: int = int(os.getenv('DELETION_MAX_RANGE_WARNING', '50000'))
        self.deletion_default_start_id: int = int(os.getenv('DELETION_DEFAULT_START_ID', '1'))
        self.deletion_default_end_id: int = int(os.getenv('DELETION_DEFAULT_END_ID', '17870'))
        self.deletion_target_group: str = os.getenv('DELETION_TARGET_GROUP', self.target_group_id)
        self.deletion_require_confirmation: bool = os.getenv('DELETION_REQUIRE_CONFIRMATION', 'true').lower() == 'true'
        self.deletion_auto_dry_run: bool = os.getenv('DELETION_AUTO_DRY_RUN', 'false').lower() == 'true'
        
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