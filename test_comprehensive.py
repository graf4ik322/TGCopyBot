#!/usr/bin/env python3
"""
Комплексные тесты для проверки исправленной функциональности.
"""

import os
import sys
import asyncio
import tempfile
import shutil
from unittest.mock import patch, MagicMock, AsyncMock

# Добавляем текущую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from utils import (
    setup_logging, RateLimiter, format_file_size, sanitize_filename,
    save_last_message_id, load_last_message_id, ProcessLock
)


def test_atomic_progress_saving():
    """Тест атомарного сохранения прогресса."""
    print("Тестируем атомарное сохранение прогресса...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        progress_file = os.path.join(temp_dir, 'test_progress.txt')
        
        # Сохраняем прогресс
        save_last_message_id(12345, progress_file)
        
        # Проверяем, что файл создан
        assert os.path.exists(progress_file), "Файл прогресса не создан"
        
        # Проверяем содержимое
        loaded_id = load_last_message_id(progress_file)
        assert loaded_id == 12345, f"Ожидался ID 12345, получен {loaded_id}"
        
        # Проверяем, что временный файл удален
        temp_file = f"{progress_file}.tmp"
        assert not os.path.exists(temp_file), "Временный файл не удален"
        
        # Тест перезаписи
        save_last_message_id(67890, progress_file)
        loaded_id = load_last_message_id(progress_file)
        assert loaded_id == 67890, f"Ожидался ID 67890, получен {loaded_id}"
    
    print("✅ Атомарное сохранение прогресса работает корректно")


def test_process_lock():
    """Тест блокировки процесса."""
    print("Тестируем блокировку процесса...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        lock_file = os.path.join(temp_dir, 'test.lock')
        
        # Первая блокировка должна быть успешной
        lock1 = ProcessLock(lock_file)
        assert lock1.acquire(), "Не удалось получить первую блокировку"
        
        # Вторая блокировка должна провалиться
        lock2 = ProcessLock(lock_file)
        assert not lock2.acquire(), "Вторая блокировка не должна была быть получена"
        
        # Освобождаем первую блокировку
        lock1.release()
        
        # Теперь вторая блокировка должна быть успешной
        assert lock2.acquire(), "Не удалось получить блокировку после освобождения"
        lock2.release()
        
        # Тест контекстного менеджера
        try:
            with ProcessLock(lock_file):
                # Внутри контекста блокировка должна быть активна
                lock3 = ProcessLock(lock_file)
                assert not lock3.acquire(), "Блокировка в контексте не работает"
        except Exception as e:
            assert False, f"Контекстный менеджер вызвал исключение: {e}"
        
        # После выхода из контекста блокировка должна быть освобождена
        lock4 = ProcessLock(lock_file)
        assert lock4.acquire(), "Блокировка не освобождена после контекста"
        lock4.release()
    
    print("✅ Блокировка процесса работает корректно")


def test_config_validation_edge_cases():
    """Тест граничных случаев валидации конфигурации."""
    print("Тестируем граничные случаи конфигурации...")
    
    # Тест с отрицательным API_ID
    with patch.dict(os.environ, {'API_ID': '-1'}, clear=True):
        config = Config()
        assert not config.validate(), "Отрицательный API_ID должен быть невалидным"
    
    # Тест с нулевыми значениями для опциональных полей
    valid_env = {
        'API_ID': '12345678',
        'API_HASH': 'test_hash',
        'PHONE': '+1234567890',
        'SOURCE_GROUP_ID': '@test_source',
        'TARGET_GROUP_ID': '@test_target',
        'DELAY_SECONDS': '0',
        'MESSAGES_PER_HOUR': '0'
    }
    
    with patch.dict(os.environ, valid_env, clear=True):
        config = Config()
        assert config.validate(), "Валидная конфигурация с нулевыми опциональными полями"
        assert config.delay_seconds == 0
        assert config.messages_per_hour == 0
    
    # Тест с очень большими значениями
    large_env = valid_env.copy()
    large_env.update({
        'DELAY_SECONDS': '3600',  # 1 час
        'MESSAGES_PER_HOUR': '10000'  # Очень много
    })
    
    with patch.dict(os.environ, large_env, clear=True):
        config = Config()
        assert config.validate(), "Конфигурация с большими значениями должна быть валидной"
    
    print("✅ Граничные случаи конфигурации обработаны корректно")


async def test_rate_limiter_edge_cases():
    """Тест граничных случаев ограничителя скорости."""
    print("Тестируем граничные случаи ограничителя скорости...")
    
    # Тест с очень малыми лимитами
    rate_limiter = RateLimiter(messages_per_hour=1, delay_seconds=0.1)
    
    import time
    start_time = time.time()
    
    # Первый вызов
    await rate_limiter.wait_if_needed()
    first_time = time.time()
    
    # Второй вызов должен подождать
    await rate_limiter.wait_if_needed()
    second_time = time.time()
    
    # Проверяем минимальную задержку
    delay = second_time - first_time
    assert delay >= 0.09, f"Задержка слишком мала: {delay}"
    
    # Тест с нулевыми лимитами
    zero_limiter = RateLimiter(messages_per_hour=0, delay_seconds=0)
    
    start_time = time.time()
    await zero_limiter.wait_if_needed()
    await zero_limiter.wait_if_needed()
    end_time = time.time()
    
    # С нулевыми лимитами не должно быть задержек
    total_time = end_time - start_time
    assert total_time < 0.1, f"С нулевыми лимитами не должно быть задержек: {total_time}"
    
    print("✅ Граничные случаи ограничителя скорости работают корректно")


def test_file_utilities():
    """Тест вспомогательных функций для работы с файлами."""
    print("Тестируем вспомогательные функции...")
    
    # Тест форматирования размера файла с граничными значениями
    assert format_file_size(0) == "0 B"
    assert format_file_size(1023) == "1023.0 B"
    assert format_file_size(1024) == "1.0 KB"
    assert format_file_size(1024 * 1024 - 1) == "1024.0 KB"
    assert format_file_size(1024 * 1024) == "1.0 MB"
    
    # Тест очистки имени файла с различными символами
    test_cases = [
        ("normal_file.txt", "normal_file.txt"),
        ("file<>with|bad*chars.txt", "file__with_bad_chars.txt"),
        ('file"with:quotes/and\\slashes.txt', "file_with_quotes_and_slashes.txt"),
        ("file?with|question*marks.txt", "file_with_question_marks.txt"),
        ("", ""),  # Пустая строка
        ("a" * 300, "a" * 255),  # Слишком длинное имя
    ]
    
    for original, expected in test_cases:
        result = sanitize_filename(original)
        if original == "a" * 300:
            assert len(result) <= 255, f"Имя файла не обрезано: {len(result)}"
        else:
            assert result == expected, f"Ожидалось '{expected}', получено '{result}'"
    
    print("✅ Вспомогательные функции работают корректно")


def test_error_handling():
    """Тест обработки ошибок."""
    print("Тестируем обработку ошибок...")
    
    # Тест сохранения прогресса в недоступную директорию
    with tempfile.TemporaryDirectory() as temp_dir:
        # Создаем файл вместо директории
        bad_path = os.path.join(temp_dir, 'not_a_directory')
        with open(bad_path, 'w') as f:
            f.write("test")
        
        # Пытаемся сохранить в поддиректорию файла (должно провалиться)
        bad_file_path = os.path.join(bad_path, 'progress.txt')
        
        # Это не должно вызывать исключение, только логировать ошибку
        try:
            save_last_message_id(12345, bad_file_path)
            # Проверяем, что файл не создан
            assert not os.path.exists(bad_file_path), "Файл не должен был быть создан"
        except Exception as e:
            assert False, f"save_last_message_id не должна вызывать исключения: {e}"
    
    # Тест загрузки из несуществующего файла
    result = load_last_message_id('/nonexistent/path/file.txt')
    assert result is None, "Загрузка из несуществующего файла должна вернуть None"
    
    # Тест загрузки из файла с невалидными данными
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        f.write("not_a_number")
        bad_file = f.name
    
    try:
        result = load_last_message_id(bad_file)
        assert result is None, "Загрузка невалидных данных должна вернуть None"
    finally:
        os.unlink(bad_file)
    
    print("✅ Обработка ошибок работает корректно")


async def main():
    """Запуск всех комплексных тестов."""
    print("=== Запуск комплексных тестов исправлений ===")
    
    try:
        # Тесты синхронных функций
        test_atomic_progress_saving()
        test_process_lock()
        test_config_validation_edge_cases()
        test_file_utilities()
        test_error_handling()
        
        # Тесты асинхронных функций
        await test_rate_limiter_edge_cases()
        
        print("\n🎉 Все комплексные тесты успешно пройдены!")
        print("\n✅ ИСПРАВЛЕНИЯ ПРОВЕРЕНЫ:")
        print("  - Атомарное сохранение прогресса")
        print("  - Блокировка множественного запуска")
        print("  - Правильная обработка граничных случаев")
        print("  - Надежная обработка ошибок")
        print("  - Корректная работа всех утилит")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Ошибка в комплексных тестах: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)