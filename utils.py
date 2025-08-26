"""
Вспомогательные функции для Telegram копировщика.
Включает логирование, управление задержками и обработку ошибок.
"""

import logging
import time
import asyncio
import os
import fcntl
import json
import hashlib
from typing import Optional, Union, Set, Dict, Any
from telethon.errors import FloodWaitError, PeerFloodError


def setup_logging(log_level: str = 'INFO') -> logging.Logger:
    """
    Настройка системы логирования.
    
    Args:
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Настроенный logger
    """
    logger = logging.getLogger('telegram_copier')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Удаляем существующие обработчики
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # ИСПРАВЛЕНО: Файловый обработчик в правильной директории
    data_dir = '/app/data' if os.path.exists('/app/data') else '.'
    os.makedirs(data_dir, exist_ok=True)
    log_file = os.path.join(data_dir, 'telegram_copier.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger


class RateLimiter:
    """Класс для управления ограничениями скорости отправки сообщений."""
    
    def __init__(self, messages_per_hour: int = 30, delay_seconds: int = 3):
        """
        Инициализация ограничителя скорости.
        
        Args:
            messages_per_hour: Максимальное количество сообщений в час
            delay_seconds: Минимальная задержка между сообщениями в секундах
        """
        self.messages_per_hour = messages_per_hour
        self.delay_seconds = delay_seconds
        self.message_times = []
        self.logger = logging.getLogger('telegram_copier.rate_limiter')
    
    async def wait_if_needed(self) -> None:
        """Ожидание при необходимости для соблюдения лимитов."""
        current_time = time.time()
        
        # Удаляем сообщения старше часа
        self.message_times = [t for t in self.message_times if current_time - t < 3600]
        
        # Проверяем лимит сообщений в час
        if len(self.message_times) >= self.messages_per_hour:
            wait_time = 3600 - (current_time - self.message_times[0])
            if wait_time > 0:
                self.logger.info(f"Достигнут лимит сообщений в час. Ожидание {wait_time:.1f} секунд")
                await asyncio.sleep(wait_time)
                self.message_times = []
        
        # Минимальная задержка между сообщениями
        if self.message_times and current_time - self.message_times[-1] < self.delay_seconds:
            wait_time = self.delay_seconds - (current_time - self.message_times[-1])
            await asyncio.sleep(wait_time)
    
    def record_message_sent(self) -> None:
        """
        НОВЫЙ МЕТОД: Записывает время отправки сообщения.
        Должен вызываться ПОСЛЕ успешной отправки сообщения.
        """
        self.message_times.append(time.time())


async def handle_flood_wait(error: FloodWaitError, logger: logging.Logger) -> None:
    """
    Обработка ошибки FloodWaitError.
    
    Args:
        error: Ошибка FloodWaitError от Telegram API
        logger: Logger для записи информации
    """
    wait_time = error.seconds
    logger.warning(f"FloodWaitError: ожидание {wait_time} секунд")
    await asyncio.sleep(wait_time)


def save_last_message_id(message_id: int, filename: str = 'last_message_id.txt') -> None:
    """
    ИСПРАВЛЕНО: Атомарное сохранение ID последнего обработанного сообщения в правильной директории.
    
    Args:
        message_id: ID сообщения для сохранения
        filename: Имя файла для сохранения
    """
    try:
        # ИСПРАВЛЕНО: Используем директорию данных
        data_dir = '/app/data' if os.path.exists('/app/data') else '.'
        os.makedirs(data_dir, exist_ok=True)
        
        # Полный путь к файлу в директории данных
        full_path = os.path.join(data_dir, filename)
        temp_filename = f"{full_path}.tmp"
        
        with open(temp_filename, 'w', encoding='utf-8') as f:
            f.write(str(message_id))
            f.flush()  # Принудительная запись на диск
            os.fsync(f.fileno())  # Синхронизация с диском
        
        # Атомарное переименование
        if os.path.exists(full_path):
            os.replace(temp_filename, full_path)
        else:
            os.rename(temp_filename, full_path)
            
    except Exception as e:
        logging.getLogger('telegram_copier').error(f"Ошибка сохранения ID сообщения: {e}")
        # Очищаем временный файл в случае ошибки
        try:
            if os.path.exists(f"{full_path}.tmp"):
                os.remove(f"{full_path}.tmp")
        except:
            pass


def load_last_message_id(filename: str = 'last_message_id.txt') -> Optional[int]:
    """
    ИСПРАВЛЕНО: Загрузка ID последнего обработанного сообщения из правильной директории.
    
    Args:
        filename: Имя файла для загрузки
    
    Returns:
        ID последнего сообщения или None
    """
    try:
        # ИСПРАВЛЕНО: Используем директорию данных
        data_dir = '/app/data' if os.path.exists('/app/data') else '.'
        full_path = os.path.join(data_dir, filename)
        
        if os.path.exists(full_path):
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                
                # ИСПРАВЛЕНИЕ: Валидация содержимого
                if not content:
                    logging.getLogger('telegram_copier').warning(f"Файл {full_path} пуст")
                    return None
                
                if not content.isdigit():
                    logging.getLogger('telegram_copier').error(f"Некорректное содержимое файла {full_path}: '{content}'")
                    return None
                
                message_id = int(content)
                
                # Дополнительная валидация: ID сообщения должен быть положительным
                if message_id <= 0:
                    logging.getLogger('telegram_copier').error(f"Некорректный ID сообщения: {message_id}")
                    return None
                
                logging.getLogger('telegram_copier').info(f"Загружен ID последнего сообщения: {message_id}")
                return message_id
                
    except ValueError as e:
        logging.getLogger('telegram_copier').error(f"Ошибка преобразования ID сообщения: {e}")
    except Exception as e:
        logging.getLogger('telegram_copier').error(f"Ошибка загрузки ID сообщения: {e}")
    
    return None


def format_file_size(size_bytes: int) -> str:
    """
    Форматирование размера файла в читаемый вид.
    
    Args:
        size_bytes: Размер в байтах
    
    Returns:
        Отформатированная строка размера
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    size = float(size_bytes)
    
    while size >= 1024.0 and i < len(size_names) - 1:
        size /= 1024.0
        i += 1
    
    return f"{size:.1f} {size_names[i]}"


def sanitize_filename(filename: str) -> str:
    """
    Очистка имени файла от недопустимых символов.
    
    Args:
        filename: Исходное имя файла
    
    Returns:
        Очищенное имя файла
    """
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Ограничиваем длину имени файла
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255-len(ext)] + ext
    
    return filename


class ProgressTracker:
    """Класс для отслеживания прогресса копирования."""
    
    def __init__(self, total_messages: int):
        """
        Инициализация трекера прогресса.
        
        Args:
            total_messages: Общее количество сообщений для обработки
        """
        self.total_messages = total_messages
        self.processed_messages = 0
        self.failed_messages = 0
        self.start_time = time.time()
        self.logger = logging.getLogger('telegram_copier.progress')
    
    def update(self, success: bool = True) -> None:
        """
        Обновление прогресса.
        ИСПРАВЛЕНО: Добавлена защита от переполнения прогресса.
        
        Args:
            success: Успешно ли обработано сообщение
        """
        # КРИТИЧЕСКАЯ ЗАЩИТА: Не позволяем превысить общее количество
        if self.processed_messages < self.total_messages:
            self.processed_messages += 1
            if not success:
                self.failed_messages += 1
        else:
            # Логируем предупреждение если пытаемся превысить лимит
            self.logger.warning(f"Попытка увеличить прогресс сверх лимита: {self.processed_messages}/{self.total_messages}")
        
        # Логируем прогресс каждые 10 сообщений
        if self.processed_messages % 10 == 0:
            self._log_progress()
    
    def _log_progress(self) -> None:
        """
        Логирование текущего прогресса.
        ИСПРАВЛЕНО: Улучшена логика расчета времени и процентов.
        """
        elapsed_time = time.time() - self.start_time
        progress_percent = (self.processed_messages / self.total_messages) * 100 if self.total_messages > 0 else 0
        
        # Ограничиваем прогресс до 100%
        progress_percent = min(progress_percent, 100.0)
        
        if self.processed_messages > 0 and elapsed_time > 0:
            avg_time_per_message = elapsed_time / self.processed_messages
            remaining_messages = max(0, self.total_messages - self.processed_messages)
            estimated_remaining_time = avg_time_per_message * remaining_messages
            
            # Форматируем время более читабельно
            if estimated_remaining_time < 60:
                time_str = f"{estimated_remaining_time:.0f} сек"
            else:
                time_str = f"{estimated_remaining_time/60:.1f} мин"
            
            self.logger.info(
                f"📊 Прогресс: {self.processed_messages}/{self.total_messages} "
                f"({progress_percent:.1f}%), "
                f"❌ Ошибок: {self.failed_messages}, "
                f"⏱️ Осталось: {time_str}"
            )
        else:
            self.logger.info(
                f"📊 Прогресс: {self.processed_messages}/{self.total_messages} "
                f"({progress_percent:.1f}%), "
                f"❌ Ошибок: {self.failed_messages}"
            )
    
    def get_final_stats(self) -> dict:
        """
        Получение финальной статистики.
        ИСПРАВЛЕНО: Улучшена логика расчета статистики с защитой от переполнения.
        
        Returns:
            Словарь со статистикой
        """
        elapsed_time = time.time() - self.start_time
        
        # Защита от логических несоответствий
        processed_count = min(self.processed_messages, self.total_messages)
        success_count = max(0, processed_count - self.failed_messages)
        
        return {
            'total_messages': self.total_messages,
            'processed_messages': processed_count,
            'failed_messages': self.failed_messages,
            'success_rate': (success_count / processed_count * 100) if processed_count > 0 else 0,
            'elapsed_time': elapsed_time,
            'messages_per_minute': (processed_count / elapsed_time * 60) if elapsed_time > 0 else 0
        }


class MessageDeduplicator:
    """Класс для предотвращения дублирования сообщений при повторных запусках."""
    
    def __init__(self, db_file: str = 'processed_messages.json'):
        """
        Инициализация дедупликатора.
        
        Args:
            db_file: Файл для хранения хешей обработанных сообщений
        """
        self.db_file = db_file
        self.processed_hashes: Set[str] = set()
        self.logger = logging.getLogger('telegram_copier.deduplicator')
        self.load_processed_messages()
    
    def _generate_message_hash(self, message_data: Dict[str, Any]) -> str:
        """
        Генерация уникального хеша для сообщения.
        
        Args:
            message_data: Данные сообщения (текст, медиа тип, размер)
        
        Returns:
            Хеш сообщения
        """
        # Создаем строку из ключевых характеристик сообщения
        hash_string = f"{message_data.get('text', '')}{message_data.get('media_type', '')}{message_data.get('media_size', 0)}{message_data.get('date', '')}"
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    def is_message_processed(self, message) -> bool:
        """
        Проверка, было ли сообщение уже обработано.
        
        Args:
            message: Объект сообщения Telethon
        
        Returns:
            True если сообщение уже обработано, False иначе
        """
        try:
            # Подготавливаем данные для хеширования
            message_data = {
                'text': message.message or '',
                'date': str(message.date) if message.date else '',
                'media_type': type(message.media).__name__ if message.media else '',
                'media_size': getattr(message.media.document, 'size', 0) if hasattr(message.media, 'document') else 0
            }
            
            message_hash = self._generate_message_hash(message_data)
            return message_hash in self.processed_hashes
            
        except Exception as e:
            self.logger.warning(f"Ошибка проверки дедупликации: {e}")
            return False
    
    def mark_message_processed(self, message) -> None:
        """
        Отметить сообщение как обработанное.
        
        Args:
            message: Объект сообщения Telethon
        """
        try:
            message_data = {
                'text': message.message or '',
                'date': str(message.date) if message.date else '',
                'media_type': type(message.media).__name__ if message.media else '',
                'media_size': getattr(message.media.document, 'size', 0) if hasattr(message.media, 'document') else 0
            }
            
            message_hash = self._generate_message_hash(message_data)
            self.processed_hashes.add(message_hash)
            
            # Сохраняем каждые 10 новых сообщений для производительности
            if len(self.processed_hashes) % 10 == 0:
                self.save_processed_messages()
                
        except Exception as e:
            self.logger.error(f"Ошибка сохранения хеша сообщения: {e}")
    
    def load_processed_messages(self) -> None:
        """Загрузка списка обработанных сообщений из файла."""
        try:
            if os.path.exists(self.db_file):
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.processed_hashes = set(data.get('hashes', []))
                    self.logger.info(f"Загружено {len(self.processed_hashes)} хешей обработанных сообщений")
            else:
                self.logger.info("Файл дедупликации не найден, создаем новый")
                
        except Exception as e:
            self.logger.error(f"Ошибка загрузки данных дедупликации: {e}")
            self.processed_hashes = set()
    
    def save_processed_messages(self) -> None:
        """Сохранение списка обработанных сообщений в файл."""
        try:
            data = {
                'hashes': list(self.processed_hashes),
                'last_updated': time.time()
            }
            
            # Атомарная запись
            temp_file = f"{self.db_file}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(temp_file, self.db_file)
            self.logger.debug(f"Сохранено {len(self.processed_hashes)} хешей")
            
        except Exception as e:
            self.logger.error(f"Ошибка сохранения данных дедупликации: {e}")
    
    def cleanup_old_hashes(self, max_age_days: int = 30) -> None:
        """
        Очистка старых хешей для предотвращения бесконечного роста файла.
        
        Args:
            max_age_days: Максимальный возраст хешей в днях
        """
        try:
            if os.path.exists(self.db_file):
                with open(self.db_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                last_updated = data.get('last_updated', 0)
                current_time = time.time()
                
                # Если файл старше указанного периода, очищаем его
                if current_time - last_updated > max_age_days * 24 * 3600:
                    self.logger.info(f"Очистка старых хешей (старше {max_age_days} дней)")
                    self.processed_hashes = set()
                    self.save_processed_messages()
                    
        except Exception as e:
            self.logger.error(f"Ошибка очистки старых хешей: {e}")


class ProcessLock:
    """Класс для предотвращения запуска нескольких экземпляров одновременно."""
    
    def __init__(self, lock_file: str = 'telegram_copier.lock'):
        """
        ИСПРАВЛЕНО: Инициализация блокировки процесса в правильной директории.
        
        Args:
            lock_file: Имя файла блокировки
        """
        # ИСПРАВЛЕНО: Используем директорию данных для lock файла
        data_dir = '/app/data' if os.path.exists('/app/data') else '.'
        os.makedirs(data_dir, exist_ok=True)
        self.lock_file = os.path.join(data_dir, lock_file)
        self.lock_fd = None
        self.logger = logging.getLogger('telegram_copier.lock')
    
    def acquire(self) -> bool:
        """
        Получение блокировки.
        
        Returns:
            True если блокировка получена, False если уже заблокировано
        """
        try:
            self.lock_fd = open(self.lock_file, 'w')
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            
            # Записываем PID процесса
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            
            self.logger.info(f"Блокировка получена: {self.lock_file}")
            return True
            
        except (IOError, OSError) as e:
            self.logger.warning(f"Не удалось получить блокировку: {e}")
            if self.lock_fd:
                self.lock_fd.close()
                self.lock_fd = None
            return False
    
    def release(self) -> None:
        """Освобождение блокировки."""
        if self.lock_fd:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
                os.remove(self.lock_file)
                self.logger.info("Блокировка освобождена")
            except Exception as e:
                self.logger.error(f"Ошибка освобождения блокировки: {e}")
            finally:
                self.lock_fd = None
    
    def __enter__(self):
        """Контекстный менеджер для автоматического управления блокировкой."""
        if not self.acquire():
            raise RuntimeError("Не удалось получить блокировку процесса. Возможно, другой экземпляр уже запущен.")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Автоматическое освобождение блокировки при выходе из контекста."""
        self.release()


class PerformanceMonitor:
    """Класс для мониторинга производительности в реальном времени."""
    
    def __init__(self):
        """Инициализация монитора производительности."""
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.messages_processed = 0
        self.messages_successful = 0
        self.messages_failed = 0
        self.bytes_processed = 0
        self.logger = logging.getLogger('telegram_copier.monitor')
        
        # Метрики производительности
        self.metrics = {
            'messages_per_minute': 0.0,
            'bytes_per_second': 0.0,
            'success_rate': 0.0,
            'average_message_size': 0.0,
            'uptime_seconds': 0.0
        }
    
    def record_message_processed(self, success: bool, message_size: int = 0) -> None:
        """
        Записать обработанное сообщение.
        
        Args:
            success: Успешно ли обработано сообщение
            message_size: Размер сообщения в байтах
        """
        self.messages_processed += 1
        self.bytes_processed += message_size
        
        if success:
            self.messages_successful += 1
        else:
            self.messages_failed += 1
        
        self._update_metrics()
        
        # Отчет каждые 100 сообщений
        if self.messages_processed % 100 == 0:
            self.log_performance_report()
    
    def _update_metrics(self) -> None:
        """Обновление метрик производительности."""
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        if elapsed_time > 0:
            self.metrics['messages_per_minute'] = (self.messages_processed / elapsed_time) * 60
            self.metrics['bytes_per_second'] = self.bytes_processed / elapsed_time
            self.metrics['uptime_seconds'] = elapsed_time
        
        if self.messages_processed > 0:
            self.metrics['success_rate'] = (self.messages_successful / self.messages_processed) * 100
            self.metrics['average_message_size'] = self.bytes_processed / self.messages_processed
    
    def log_performance_report(self) -> None:
        """Логирование отчета о производительности."""
        self._update_metrics()
        
        self.logger.info("=== ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ ===")
        self.logger.info(f"Обработано сообщений: {self.messages_processed}")
        self.logger.info(f"Успешно: {self.messages_successful} | Ошибок: {self.messages_failed}")
        self.logger.info(f"Успешность: {self.metrics['success_rate']:.1f}%")
        self.logger.info(f"Скорость: {self.metrics['messages_per_minute']:.1f} сообщений/мин")
        self.logger.info(f"Пропускная способность: {format_file_size(int(self.metrics['bytes_per_second']))}/с")
        self.logger.info(f"Средний размер сообщения: {format_file_size(int(self.metrics['average_message_size']))}")
        self.logger.info(f"Время работы: {self.metrics['uptime_seconds']:.0f} сек")
        self.logger.info("=====================================")
    
    def get_metrics(self) -> Dict[str, float]:
        """
        Получить текущие метрики производительности.
        
        Returns:
            Словарь с метриками
        """
        self._update_metrics()
        return self.metrics.copy()
    
    def get_final_report(self) -> Dict[str, Any]:
        """
        Получить финальный отчет.
        
        Returns:
            Детальный отчет о производительности
        """
        self._update_metrics()
        
        return {
            'total_messages': self.messages_processed,
            'successful_messages': self.messages_successful,
            'failed_messages': self.messages_failed,
            'total_bytes': self.bytes_processed,
            'metrics': self.metrics,
            'efficiency_score': self._calculate_efficiency_score()
        }
    
    def _calculate_efficiency_score(self) -> float:
        """
        Вычисление оценки эффективности (0-100).
        
        Returns:
            Оценка эффективности
        """
        if self.messages_processed == 0:
            return 0.0
        
        # Базовая оценка на основе успешности
        base_score = self.metrics['success_rate']
        
        # Бонус за скорость (если больше 10 сообщений в минуту)
        speed_bonus = min(self.metrics['messages_per_minute'] / 10, 1.0) * 10
        
        # Штраф за слишком много ошибок
        error_penalty = (self.messages_failed / self.messages_processed) * 20
        
        efficiency = base_score + speed_bonus - error_penalty
        return max(0.0, min(100.0, efficiency))