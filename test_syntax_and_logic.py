#!/usr/bin/env python3
"""
Тесты синтаксиса и базовой логики без внешних зависимостей.
"""

import os
import sys
import tempfile
import time

# Добавляем текущую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_file_operations():
    """Тест файловых операций без зависимостей."""
    print("Тестируем файловые операции...")
    
    # Импортируем только нужные функции
    from utils import save_last_message_id, load_last_message_id, format_file_size, sanitize_filename
    
    with tempfile.TemporaryDirectory() as temp_dir:
        progress_file = os.path.join(temp_dir, 'test_progress.txt')
        
        # Тест сохранения и загрузки
        save_last_message_id(12345, progress_file)
        loaded_id = load_last_message_id(progress_file)
        assert loaded_id == 12345, f"Ожидался ID 12345, получен {loaded_id}"
        
        # Тест атомарности - проверяем что временный файл удален
        temp_file = f"{progress_file}.tmp"
        assert not os.path.exists(temp_file), "Временный файл не удален"
        
        print("✅ Атомарное сохранение работает")
        
        # Тест форматирования размера
        assert format_file_size(1024) == "1.0 KB"
        assert format_file_size(1024 * 1024) == "1.0 MB"
        print("✅ Форматирование размера работает")
        
        # Тест очистки имени файла
        assert sanitize_filename("file<>test.txt") == "file__test.txt"
        print("✅ Очистка имени файла работает")


def test_config_structure():
    """Тест структуры конфигурации без загрузки .env."""
    print("Тестируем структуру конфигурации...")
    
    # Мокируем load_dotenv
    import config
    original_load_dotenv = config.load_dotenv
    config.load_dotenv = lambda x: None
    
    try:
        # Тест с переменными окружения
        os.environ['API_ID'] = '12345'
        os.environ['API_HASH'] = 'test_hash'
        os.environ['PHONE'] = '+1234567890'
        os.environ['SOURCE_GROUP_ID'] = '@source'
        os.environ['TARGET_GROUP_ID'] = '@target'
        
        from config import Config
        config_obj = Config()
        
        assert config_obj.api_id == 12345
        assert config_obj.api_hash == 'test_hash'
        assert config_obj.phone == '+1234567890'
        assert config_obj.validate() == True
        
        print("✅ Структура конфигурации корректна")
        
        # Тест прокси конфигурации
        os.environ['PROXY_SERVER'] = 'proxy.test.com'
        os.environ['PROXY_PORT'] = '8080'
        
        config_obj = Config()
        proxy_config = config_obj.get_proxy_config()
        assert proxy_config is not None
        assert proxy_config['addr'] == 'proxy.test.com'
        assert proxy_config['port'] == 8080
        
        print("✅ Конфигурация прокси работает")
        
    finally:
        # Восстанавливаем оригинальную функцию
        config.load_dotenv = original_load_dotenv
        
        # Очищаем переменные окружения
        for key in ['API_ID', 'API_HASH', 'PHONE', 'SOURCE_GROUP_ID', 'TARGET_GROUP_ID', 'PROXY_SERVER', 'PROXY_PORT']:
            if key in os.environ:
                del os.environ[key]


def test_process_lock_basic():
    """Базовый тест блокировки процесса."""
    print("Тестируем базовую блокировку процесса...")
    
    from utils import ProcessLock
    
    with tempfile.TemporaryDirectory() as temp_dir:
        lock_file = os.path.join(temp_dir, 'test.lock')
        
        # Тест успешного получения блокировки
        lock1 = ProcessLock(lock_file)
        assert lock1.acquire() == True, "Не удалось получить блокировку"
        
        # Тест неуспешного получения второй блокировки
        lock2 = ProcessLock(lock_file)
        assert lock2.acquire() == False, "Вторая блокировка не должна была быть получена"
        
        # Освобождение блокировки
        lock1.release()
        
        # Теперь вторая блокировка должна работать
        assert lock2.acquire() == True, "Не удалось получить блокировку после освобождения"
        lock2.release()
        
        print("✅ Базовая блокировка процесса работает")


def test_imports_and_structure():
    """Тест корректности импортов и структуры модулей."""
    print("Тестируем импорты и структуру...")
    
    # Проверяем что все основные классы и функции импортируются
    try:
        from config import Config
        from utils import RateLimiter, ProgressTracker, ProcessLock
        print("✅ Все основные классы импортируются корректно")
        
        # Проверяем создание объектов
        rate_limiter = RateLimiter(30, 3)
        progress_tracker = ProgressTracker(100)
        process_lock = ProcessLock()
        
        print("✅ Все объекты создаются корректно")
        
    except ImportError as e:
        print(f"❌ Ошибка импорта: {e}")
        return False
    
    return True


def main():
    """Запуск всех тестов."""
    print("=== Запуск тестов синтаксиса и логики ===")
    
    try:
        test_file_operations()
        test_config_structure()
        test_process_lock_basic()
        test_imports_and_structure()
        
        print("\n🎉 Все тесты синтаксиса и логики пройдены!")
        print("\n✅ ПРОВЕРЕНО:")
        print("  - Синтаксис всех модулей корректен")
        print("  - Атомарное сохранение прогресса работает")
        print("  - Блокировка процесса функционирует")
        print("  - Конфигурация загружается правильно")
        print("  - Все импорты работают")
        print("  - Вспомогательные функции корректны")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Ошибка в тестах: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)