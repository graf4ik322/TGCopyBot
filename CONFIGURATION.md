# 📋 Полное руководство по конфигурации

Этот документ описывает все доступные параметры конфигурации для Telegram Copier и модуля удаления сообщений.

## 🚀 Быстрый старт

1. Скопируйте `.env.template` в `.env`:
   ```bash
   cp .env.template .env
   ```

2. Заполните обязательные параметры в `.env`

3. Запустите нужный скрипт

## 📝 Обязательные параметры

### API конфигурация
```env
API_ID=12345678                              # Ваш API ID от Telegram
API_HASH=abcdef1234567890abcdef1234567890    # Ваш API Hash от Telegram  
PHONE=+1234567890                            # Ваш номер телефона
```

### Группы
```env
SOURCE_GROUP_ID=@source_group                # Исходная группа для копирования
TARGET_GROUP_ID=@target_group                # Целевая группа для копирования
```

## ⚙️ Настройки копирования сообщений

### Поведение копирования
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DELAY_SECONDS` | `3` | Задержка между копированием сообщений (секунды) |
| `MESSAGES_PER_HOUR` | `30` | Максимум сообщений для копирования в час |
| `DRY_RUN` | `false` | Режим симуляции без реального копирования |

### Сессия и хранение
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `SESSION_NAME` | `telegram_copier` | Имя файла сессии (без .session) |
| `RESUME_FILE` | `last_message_id.txt` | Файл для сохранения прогресса |

### Трекинг сообщений
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `USE_MESSAGE_TRACKER` | `true` | Включить детальный трекинг сообщений |
| `TRACKER_FILE` | `copied_messages.json` | Файл для детального трекинга |
| `ADD_DEBUG_TAGS` | `false` | Добавлять отладочные теги к сообщениям |
| `FLATTEN_STRUCTURE` | `false` | Превращать вложенные ответы в плоскую структуру |

## 🗑️ Настройки удаления сообщений

### Целевая группа и диапазон
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DELETION_TARGET_GROUP` | `TARGET_GROUP_ID` | Группа для удаления сообщений |
| `DELETION_DEFAULT_START_ID` | `1` | Начальный ID сообщения для удаления |
| `DELETION_DEFAULT_END_ID` | `17870` | Конечный ID сообщения для удаления |

### Производительность удаления
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DELETION_BATCH_SIZE` | `100` | Количество сообщений в одном батче |
| `DELETION_MESSAGES_PER_HOUR` | `6000` | Максимум сообщений для удаления в час |
| `DELETION_DELAY_SECONDS` | `1` | Задержка между батчами удаления |

### Безопасность удаления
| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `DELETION_TIMEOUT_SECONDS` | `30` | Таймаут подключения для операций удаления |
| `DELETION_MAX_RANGE_WARNING` | `50000` | Порог предупреждения о больших диапазонах |
| `DELETION_REQUIRE_CONFIRMATION` | `true` | Требовать подтверждение перед удалением |
| `DELETION_AUTO_DRY_RUN` | `false` | Автоматический режим симуляции удаления |

## 🌐 Настройки прокси (опционально)

```env
PROXY_SERVER=your_proxy_host     # Хост прокси сервера
PROXY_PORT=1080                  # Порт прокси сервера  
PROXY_USERNAME=username          # Имя пользователя прокси
PROXY_PASSWORD=password          # Пароль прокси
```

## 📊 Примеры конфигураций

### Быстрое удаление (максимальная скорость)
```env
DELETION_BATCH_SIZE=100
DELETION_MESSAGES_PER_HOUR=6000
DELETION_DELAY_SECONDS=1
DELETION_REQUIRE_CONFIRMATION=false
```

### Безопасное удаление (консервативный подход)
```env
DELETION_BATCH_SIZE=50
DELETION_MESSAGES_PER_HOUR=3000
DELETION_DELAY_SECONDS=2
DELETION_REQUIRE_CONFIRMATION=true
DELETION_MAX_RANGE_WARNING=10000
```

### Тестирование (только симуляция)
```env
DRY_RUN=true
DELETION_AUTO_DRY_RUN=true
DELETION_REQUIRE_CONFIRMATION=false
ADD_DEBUG_TAGS=true
```

### Медленное копирование (для избежания лимитов)
```env
DELAY_SECONDS=5
MESSAGES_PER_HOUR=20
USE_MESSAGE_TRACKER=true
```

## 🎯 Расчет времени выполнения

### Формула для копирования
```
Время = (Количество сообщений × DELAY_SECONDS) + overhead
```

### Формула для удаления
```
Батчей = ceil(Количество сообщений / DELETION_BATCH_SIZE)
Время = Батчей × DELETION_DELAY_SECONDS
```

### Примеры
| Операция | Сообщений | Настройки | Время |
|----------|-----------|-----------|-------|
| Копирование | 1000 | DELAY_SECONDS=3 | ~50 минут |
| Копирование | 1000 | DELAY_SECONDS=1 | ~17 минут |
| Удаление | 17870 | Batch=100, Delay=1s | ~3 минуты |
| Удаление | 17870 | Batch=50, Delay=2s | ~12 минут |

## 🛡️ Рекомендации по безопасности

### Для продакшена
- Всегда используйте `DELETION_REQUIRE_CONFIRMATION=true`
- Установите разумный `DELETION_MAX_RANGE_WARNING`
- Начинайте с `DRY_RUN=true` для тестирования
- Используйте консервативные значения delay

### Для разработки
- Используйте `DELETION_AUTO_DRY_RUN=true`
- Включите `ADD_DEBUG_TAGS=true`
- Установите `USE_MESSAGE_TRACKER=true`

### Для массовых операций
- Увеличьте `DELETION_BATCH_SIZE` до 100
- Уменьшите `DELETION_DELAY_SECONDS` до 1
- Отключите `DELETION_REQUIRE_CONFIRMATION` (осторожно!)

## 🔧 Диагностика проблем

### Медленная работа
- Уменьшите `DELAY_SECONDS` / `DELETION_DELAY_SECONDS`
- Увеличьте `MESSAGES_PER_HOUR` / `DELETION_MESSAGES_PER_HOUR`
- Увеличьте `DELETION_BATCH_SIZE`

### Ошибки лимитов Telegram
- Увеличьте `DELAY_SECONDS` / `DELETION_DELAY_SECONDS`  
- Уменьшите `MESSAGES_PER_HOUR` / `DELETION_MESSAGES_PER_HOUR`
- Уменьшите `DELETION_BATCH_SIZE`

### Ошибки соединения
- Увеличьте `DELETION_TIMEOUT_SECONDS`
- Настройте прокси
- Проверьте интернет-соединение

## 📱 Командная строка vs .env

Параметры командной строки переопределяют настройки .env:

```bash
# Использует настройки из .env
python3 cleanup_group.py

# Переопределяет настройки .env
python3 message_deleter.py @custom_group 1 5000 --dry-run
```

## 🔄 Переменные окружения

Все параметры .env можно задавать как переменные окружения:

```bash
export DELETION_BATCH_SIZE=50
export DELETION_DELAY_SECONDS=2
python3 cleanup_group.py
```

## 📋 Валидация конфигурации

Скрипты автоматически проверяют:
- Наличие обязательных параметров
- Корректность числовых значений
- Валидность boolean параметров
- Доступность групп/каналов

При ошибках конфигурации выводятся подробные сообщения с инструкциями по исправлению.