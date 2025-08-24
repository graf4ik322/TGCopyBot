#!/usr/bin/env python3
"""
Главный модуль Telegram копировщика постов.
Обеспечивает авторизацию, инициализацию и запуск процесса копирования.
"""

import asyncio
import sys
import signal
import os
from typing import Optional
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError

from config import Config
from utils import setup_logging, RateLimiter, load_last_message_id, ProcessLock
from copier import TelegramCopier


class TelegramCopierApp:
    """Главный класс приложения для копирования Telegram сообщений."""
    
    def __init__(self):
        """Инициализация приложения."""
        self.config = Config()
        self.logger = setup_logging()
        self.client: Optional[TelegramClient] = None
        self.copier: Optional[TelegramCopier] = None
        self.running = False
        
        # Обработчики сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения."""
        self.logger.info(f"Получен сигнал {signum}, завершаем работу...")
        self.running = False
    
    async def _validate_session_by_auth(self, session_path: str) -> bool:
        """
        ИСПРАВЛЕНО: Валидация сессии через проверку авторизации.
        Проверяет, можно ли авторизоваться с данной сессией, а не только целостность файла.
        
        Args:
            session_path: Путь к файлу сессии (без расширения .session)
            
        Returns:
            True если сессия позволяет авторизоваться, False если сессия разорвана/неактуальна
        """
        if not os.path.exists(f"{session_path}.session"):
            self.logger.debug("Файл сессии не существует")
            return False
            
        try:
            # Создаем временный клиент для проверки сессии
            temp_client = TelegramClient(
                session_path,
                self.config.api_id, 
                self.config.api_hash,
                proxy=self.config.get_proxy_config()
            )
            
            self.logger.info("Проверяем валидность сессии через авторизацию...")
            
            try:
                # Пытаемся подключиться с существующей сессией
                await temp_client.start()
                
                # ГЛАВНОЕ: Проверяем, можем ли мы авторизоваться
                if await temp_client.is_user_authorized():
                    # Дополнительная проверка - пытаемся получить информацию о себе
                    try:
                        me = await temp_client.get_me()
                        if me and me.id:
                            self.logger.info(f"✅ Сессия валидна. Авторизован как: {me.first_name} (@{me.username or 'no_username'})")
                            await temp_client.disconnect()
                            return True
                        else:
                            self.logger.warning("Сессия существует, но данные пользователя недоступны")
                            await temp_client.disconnect()
                            return False
                    except Exception as e:
                        self.logger.warning(f"Не удалось получить данные пользователя: {e}")
                        await temp_client.disconnect()
                        return False
                else:
                    self.logger.warning("❌ Сессия неактуальна - авторизация не проходит")
                    await temp_client.disconnect()
                    return False
                    
            except Exception as e:
                self.logger.warning(f"Ошибка при проверке авторизации с сессией: {e}")
                try:
                    await temp_client.disconnect()
                except:
                    pass
                return False
                
        except Exception as e:
            self.logger.warning(f"Ошибка создания временного клиента для валидации: {e}")
            return False
    
    async def initialize(self) -> bool:
        """
        Инициализация клиента и проверка конфигурации.
        
        Returns:
            True если инициализация успешна, False иначе
        """
        # Валидация конфигурации
        if not self.config.validate():
            self.logger.error("Конфигурация невалидна")
            return False
        
        try:
            # ИСПРАВЛЕНО: Сначала проверяем существование сессии, ПОТОМ создаем клиент
            proxy_config = self.config.get_proxy_config()
            
            # Используем /app/data для всех файлов данных
            data_dir = '/app/data' if os.path.exists('/app/data') else '.'
            os.makedirs(data_dir, exist_ok=True)
            
            # Устанавливаем правильные права доступа для директории данных
            try:
                os.chmod(data_dir, 0o755)
                self.logger.info(f"Установлены права доступа для директории: {data_dir}")
            except Exception as e:
                self.logger.warning(f"Не удалось установить права доступа: {e}")
            
            # Путь к сессии в директории данных
            session_path = os.path.join(data_dir, self.config.session_name)
            session_file = f"{session_path}.session"
            
            # ИСПРАВЛЕНО: Проверяем существование сессии ДО создания клиента
            session_exists = os.path.exists(session_file)
            if session_exists:
                self.logger.info(f"Найден существующий файл сессии: {session_file}")
                # Проверяем валидность существующей сессии
                try:
                    file_size = os.path.getsize(session_file)
                    if file_size < 100:
                        self.logger.warning(f"Файл сессии слишком мал ({file_size} байт), удаляем")
                        os.remove(session_file)
                        session_exists = False
                    else:
                        # Проверяем SQLite структуру
                        import sqlite3
                        try:
                            conn = sqlite3.connect(session_file)
                            conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                            conn.close()
                            self.logger.info("Существующая сессия прошла валидацию")
                        except sqlite3.Error as e:
                            self.logger.warning(f"Существующая сессия повреждена: {e}, удаляем")
                            os.remove(session_file)
                            session_exists = False
                except Exception as e:
                    self.logger.warning(f"Ошибка проверки существующей сессии: {e}, удаляем")
                    try:
                        os.remove(session_file)
                    except:
                        pass
                    session_exists = False
            else:
                self.logger.info("Файл сессии не найден, будет создан новый")
            
            self.logger.info(f"Создаем клиент с сессией: {session_path}")
            self.logger.info(f"Текущая рабочая директория: {os.getcwd()}")
            self.logger.info(f"Директория данных: {data_dir}")
            
            self.client = TelegramClient(
                session_path,
                self.config.api_id,
                self.config.api_hash,
                proxy=proxy_config,
                # Добавляем уникальные параметры для предотвращения конфликтов сессий
                device_model="Telegram Copier Script",
                app_version="1.0.0",
                system_version="Linux"
            )
            
            self.logger.info("Telegram клиент создан")
            
            # Сохраняем информацию о том, была ли сессия до создания клиента
            self.had_existing_session = session_exists
            
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка создания клиента: {e}")
            return False
    
    async def authenticate(self) -> bool:
        """
        Авторизация в Telegram.
        
        Returns:
            True если авторизация успешна, False иначе
        """
        try:
            # ИСПРАВЛЕНО: НЕ проверяем файл сессии после создания клиента
            # TelegramClient сам создает пустой файл при инициализации!
            
            # ИСПРАВЛЕНО: Добавляем таймаут для предотвращения зависания
            try:
                self.logger.info("Запускаем Telegram клиент...")
                await asyncio.wait_for(self.client.start(), timeout=30.0)
                self.logger.info("Telegram клиент успешно запущен")
            except asyncio.TimeoutError:
                self.logger.error("Таймаут при запуске клиента (30 сек). Возможно, проблема с сессией.")
                # Удаляем проблемную сессию
                data_dir = '/app/data' if os.path.exists('/app/data') else '.'
                session_file = os.path.join(data_dir, f"{self.config.session_name}.session")
                if os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                        self.logger.info("Удалена проблемная сессия после таймаута")
                    except:
                        pass
                return False
            except Exception as e:
                self.logger.error(f"Ошибка запуска клиента: {e}")
                return False
            
            # Проверяем, авторизованы ли мы уже (только если была существующая сессия)
            if self.had_existing_session and await self.client.is_user_authorized():
                try:
                    # УЛУЧШЕНО: Дополнительная проверка валидности авторизации
                    me = await self.client.get_me()
                    if me and me.id:
                        self.logger.info(f"Авторизован как: {me.first_name} {me.last_name or ''} (@{me.username or 'no_username'})")
                        self.logger.info("✅ Используется валидная существующая сессия")
                        return True
                    else:
                        self.logger.warning("Сессия существует, но данные пользователя недоступны")
                        # Не возвращаем False, а продолжаем с новой авторизацией
                except Exception as e:
                    self.logger.error(f"Ошибка проверки авторизации: {e}")
                    self.logger.warning("Сессия повреждена, требуется повторная авторизация")
            elif self.had_existing_session:
                self.logger.warning("Существующая сессия неактуальна, требуется повторная авторизация")
            else:
                self.logger.info("Новая сессия, требуется авторизация")
            
            # Если не авторизованы, начинаем процесс авторизации
            self.logger.info("Начинаем процесс авторизации")
            
            # Отправляем код
            await self.client.send_code_request(self.config.phone)
            self.logger.info(f"Код отправлен на номер {self.config.phone}")
            
            # Проверяем поддержку интерактивного ввода
            code = os.getenv('TELEGRAM_CODE')
            
            if not code:
                # Проверяем, можем ли мы использовать интерактивный ввод
                if sys.stdin.isatty() and sys.stdout.isatty():
                    try:
                        code = input("Введите код из SMS: ").strip()
                        if not code:
                            self.logger.error("Код не введен")
                            return False
                    except (EOFError, KeyboardInterrupt):
                        self.logger.error("Ввод кода прерван пользователем")
                        return False
                else:
                    self.logger.error(
                        "Не найден код авторизации в переменной окружения TELEGRAM_CODE.\n"
                        "Для Docker используйте: TELEGRAM_CODE=12345 docker-compose up\n"
                        "Или запустите локально для интерактивной авторизации: python main.py"
                    )
                    return False
            
            try:
                await self.client.sign_in(self.config.phone, code)
                self.logger.info("💾 Новая сессия создана и сохранена")
                
            except SessionPasswordNeededError:
                # Требуется двухфакторная аутентификация
                password = os.getenv('TELEGRAM_PASSWORD')
                
                if not password:
                    # Проверяем поддержку интерактивного ввода для 2FA
                    if sys.stdin.isatty() and sys.stdout.isatty():
                        try:
                            import getpass
                            password = getpass.getpass("Введите пароль двухфакторной аутентификации: ").strip()
                            if not password:
                                self.logger.error("Пароль не введен")
                                return False
                        except (EOFError, KeyboardInterrupt):
                            self.logger.error("Ввод пароля прерван пользователем")
                            return False
                    else:
                        self.logger.error(
                            "Требуется пароль двухфакторной аутентификации в переменной TELEGRAM_PASSWORD.\n"
                            "Для Docker используйте: TELEGRAM_PASSWORD=your_password docker-compose up\n"
                            "Или запустите локально для интерактивной авторизации: python main.py"
                        )
                        return False
                
                await self.client.sign_in(password=password)
                self.logger.info("Авторизация с двухфакторной аутентификацией успешна")
                self.logger.info("💾 Новая сессия создана и сохранена")
            
            except PhoneCodeInvalidError:
                self.logger.error("Неверный код авторизации")
                return False
            
            except PhoneNumberInvalidError:
                self.logger.error("Неверный номер телефона")
                return False
            
            # Проверяем успешность авторизации
            if await self.client.is_user_authorized():
                me = await self.client.get_me()
                self.logger.info(f"Авторизация успешна: {me.first_name} {me.last_name or ''}")
                return True
            else:
                self.logger.error("Авторизация не удалась")
                return False
                
        except Exception as e:
            self.logger.error(f"Ошибка авторизации: {e}")
            return False
    
    async def run_copying(self) -> dict:
        """
        Запуск процесса копирования сообщений.
        
        Returns:
            Статистика копирования
        """
        try:
            # Создаем ограничитель скорости
            rate_limiter = RateLimiter(
                messages_per_hour=self.config.messages_per_hour,
                delay_seconds=self.config.delay_seconds
            )
            
            # Создаем копировщик
            self.copier = TelegramCopier(
                client=self.client,
                source_group_id=self.config.source_group_id,
                target_group_id=self.config.target_group_id,
                rate_limiter=rate_limiter,
                dry_run=self.config.dry_run,
                resume_file=self.config.resume_file,
                use_message_tracker=getattr(self.config, 'use_message_tracker', True),
                tracker_file=getattr(self.config, 'tracker_file', 'copied_messages.json'),
                add_debug_tags=getattr(self.config, 'add_debug_tags', False),
                flatten_structure=getattr(self.config, 'flatten_structure', False),
                debug_message_ids=getattr(self.config, 'debug_message_ids', False),
                batch_size=getattr(self.config, 'batch_size', 100)
            )
            
            # Проверяем, нужно ли возобновить с определенного места
            resume_from_id = load_last_message_id(self.config.resume_file)
            if resume_from_id:
                self.logger.info(f"Возобновляем копирование с сообщения ID: {resume_from_id}")
            
            # Запускаем батчевое копирование (ИСПРАВЛЕНИЕ: предотвращает проблемы с памятью)
            self.running = True
            if hasattr(self.copier, 'copy_all_messages_batch'):
                self.logger.info("🔧 Используется батчевая обработка для предотвращения проблем с памятью")
                stats = await self.copier.copy_all_messages_batch(resume_from_id)
            else:
                # Fallback на старый метод, если новый недоступен
                self.logger.warning("⚠️ Батчевая обработка недоступна, используется обычный метод")
                stats = await self.copier.copy_all_messages(resume_from_id)
            
            # Очищаем временные файлы
            if self.copier:
                self.copier.cleanup_temp_files()
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Ошибка при копировании: {e}")
            return {'error': str(e)}
    
    async def run(self) -> int:
        """
        Основной метод запуска приложения.
        
        Returns:
            Код завершения (0 - успех, 1 - ошибка)
        """
        self.logger.info("=== Запуск Telegram Copier ===")
        
        # Используем блокировку процесса для предотвращения множественного запуска
        try:
            with ProcessLock('telegram_copier.lock'):
                # Инициализация
                if not await self.initialize():
                    return 1
                
                # Авторизация
                if not await self.authenticate():
                    return 1
                
                # Запуск копирования
                stats = await self.run_copying()
                
                if 'error' in stats:
                    self.logger.error(f"Копирование завершилось с ошибкой: {stats['error']}")
                    return 1
                
                # Проверяем статус копирования
                if stats.get('status') == 'up_to_date':
                    self.logger.info("=== Копирование актуально ===")
                    self.logger.info(f"📊 Всего сообщений в канале: {stats.get('total_messages', 0)}")
                    self.logger.info(f"✅ {stats.get('message', 'Все сообщения уже скопированы')}")
                    self.logger.info("🔄 Новых сообщений для копирования не найдено")
                    self.logger.info("💡 Запустите скрипт позже, когда появятся новые сообщения")
                else:
                    # Выводим финальную статистику для обычного копирования
                    self.logger.info("=== Финальная статистика ===")
                    self.logger.info(f"Всего сообщений: {stats.get('total_messages', 0)}")
                    self.logger.info(f"Скопировано: {stats.get('copied_messages', 0)}")
                    self.logger.info(f"Ошибок: {stats.get('failed_messages', 0)}")
                    self.logger.info(f"Пропущено: {stats.get('skipped_messages', 0)}")
                    
                    # Показываем информацию о батчах, если доступна
                    if 'batches_processed' in stats:
                        self.logger.info(f"📦 Обработано батчей: {stats['batches_processed']}")
                        self.logger.info(f"📏 Размер батча: {stats.get('batch_size', 100)}")
                    
                    # Показываем проверку целевого канала, если доступна
                    if 'target_messages_count' in stats:
                        self.logger.info(f"📊 В целевом канале: {stats['target_messages_count']} сообщений")
                    
                    self.logger.info(f"Время выполнения: {stats.get('elapsed_time', 0):.1f} сек")
                    self.logger.info(f"Скорость: {stats.get('messages_per_minute', 0):.1f} сообщений/мин")
                
                if self.config.dry_run:
                    self.logger.info("=== РЕЖИМ СИМУЛЯЦИИ - реальная отправка не выполнялась ===")
                
                self.logger.info("=== Копирование завершено успешно ===")
                return 0
        
        except RuntimeError as e:
            if "блокировку процесса" in str(e):
                self.logger.error("Другой экземпляр копировщика уже запущен. Дождитесь его завершения.")
                return 1
            else:
                raise
            
        except KeyboardInterrupt:
            self.logger.info("Прервано пользователем")
            return 1
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {e}")
            return 1
            
        finally:
            # Закрываем клиент
            if self.client:
                await self.client.disconnect()
                self.logger.info("Клиент отключен")


async def main():
    """Точка входа в приложение."""
    app = TelegramCopierApp()
    exit_code = await app.run()
    sys.exit(exit_code)


def interactive_setup():
    """Интерактивная настройка конфигурации (для локального использования)."""
    print("=== Интерактивная настройка Telegram Copier ===")
    print()
    
    # Проверяем существование .env файла
    if os.path.exists('.env'):
        response = input("Файл .env уже существует. Перезаписать? (y/N): ")
        if response.lower() != 'y':
            print("Настройка отменена")
            return
    
    # Собираем данные от пользователя
    print("Введите данные для конфигурации:")
    print("(Получить API_ID и API_HASH можно на https://my.telegram.org/apps)")
    print()
    
    api_id = input("API_ID: ")
    # ИСПРАВЛЕНИЕ: API_HASH должен вводиться как пароль для безопасности
    import getpass
    api_hash = getpass.getpass("API_HASH (скрыт): ")
    phone = input("Номер телефона (с кодом страны, например +79123456789): ")
    source_group = input("ID или username исходной группы (например @my_group или -1001234567890): ")
    target_group = input("ID или username целевой группы: ")
    
    print()
    print("Дополнительные настройки (можно оставить пустыми для значений по умолчанию):")
    delay = input("Задержка между сообщениями в секундах (по умолчанию 3): ") or "3"
    messages_per_hour = input("Максимум сообщений в час (по умолчанию 30): ") or "30"
    dry_run = input("Режим симуляции без реальной отправки? (y/N): ").lower() == 'y'
    
    # Создаем .env файл
    env_content = f"""# Telegram API настройки
API_ID={api_id}
API_HASH={api_hash}
PHONE={phone}

# Настройки групп
SOURCE_GROUP_ID={source_group}
TARGET_GROUP_ID={target_group}

# Настройки поведения
DELAY_SECONDS={delay}
MESSAGES_PER_HOUR={messages_per_hour}
DRY_RUN={'true' if dry_run else 'false'}

# Дополнительные настройки
SESSION_NAME=telegram_copier
RESUME_FILE=last_message_id.txt

# Отладка
DEBUG_MESSAGE_IDS=false
"""
    
    with open('.env', 'w', encoding='utf-8') as f:
        f.write(env_content)
    
    print()
    print("Конфигурация сохранена в файл .env")
    print("Теперь вы можете запустить копировщик командой: python main.py")
    print()
    print("ВАЖНО: При первом запуске вам потребуется ввести код авторизации из Telegram")


if __name__ == '__main__':
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == 'setup':
            interactive_setup()
        elif sys.argv[1] == 'reset':
            # Сброс прогресса копирования
            print("=== Сброс прогресса копирования ===")
            files_to_remove = ['last_message_id.txt', 'message_hashes.json', 'copied_messages.json']
            
            removed_files = []
            for file_path in files_to_remove:
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                        removed_files.append(file_path)
                        print(f"✅ Удален файл: {file_path}")
                    except Exception as e:
                        print(f"❌ Ошибка удаления {file_path}: {e}")
                else:
                    print(f"ℹ️ Файл {file_path} не найден")
            
            if removed_files:
                print(f"\n🔄 Сброшен прогресс копирования")
                print("💡 Следующий запуск начнет копирование с самого начала")
            else:
                print("\n📝 Файлы прогресса не найдены - копирование и так начнется с начала")
        elif sys.argv[1] == 'status':
            # Показать статистику трекинга
            print("=== Статистика копирования ===")
            try:
                from message_tracker import MessageTracker
                tracker = MessageTracker()
                stats = tracker.get_statistics()
                print(f"📊 Отслежено сообщений: {stats['total_tracked']}")
                print(f"✅ Успешно скопировано: {stats['successfully_copied']}")
                print(f"❌ Неудачных попыток: {stats['failed_copies']}")
                print(f"🕒 Последнее обновление: {stats.get('last_updated', 'не известно')}")
                if stats.get('source_channel'):
                    print(f"📤 Исходный канал: {stats['source_channel']}")
                if stats.get('target_channel'):
                    print(f"📥 Целевой канал: {stats['target_channel']}")
            except Exception as e:
                print(f"❌ Ошибка получения статистики: {e}")
        elif sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print("=== Telegram Copier - Справка ===")
            print()
            print("Использование:")
            print("  python main.py          - Запуск копирования")
            print("  python main.py setup    - Интерактивная настройка")
            print("  python main.py status   - Показать статистику копирования")
            print("  python main.py reset    - Сброс прогресса копирования")
            print("  python main.py --help   - Показать эту справку")
            print()
            print("Примеры:")
            print("  python main.py setup    # Настроить .env файл")
            print("  python main.py          # Запустить копирование")
            print("  python main.py reset    # Начать копирование заново")
        else:
            print(f"❌ Неизвестная команда: {sys.argv[1]}")
            print("Используйте 'python main.py --help' для справки")
    else:
        asyncio.run(main())