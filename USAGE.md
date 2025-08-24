# 🚀 Быстрый старт Telegram Copier v3.0

## 📋 Установка

1. **Клонируйте репозиторий:**
```bash
git clone https://github.com/graf4ik322/TGCopyBot.git
cd TGCopyBot
```

2. **Установите зависимости:**
```bash
pip install -r requirements.txt
```

3. **Создайте файл .env:**
```env
# Telegram API данные (получите на https://my.telegram.org)
API_ID=your_api_id
API_HASH=your_api_hash
PHONE=your_phone_number

# Каналы для копирования
SOURCE_GROUP_ID=@source_channel  # или -100123456789
TARGET_GROUP_ID=@target_channel   # или -100987654321

# Настройки (опционально)
DELAY_SECONDS=3
DRY_RUN=false
SESSION_NAME=telegram_copier
```

## 🚀 Запуск

```bash
python main.py
```

## 📊 Что происходит

### Первый запуск:
1. **Сканирование** - полное сканирование исходного канала и сохранение в SQLite БД
2. **Копирование** - копирование всех постов и комментариев в хронологическом порядке

### Последующие запуски:
1. **Загрузка из БД** - мгновенная загрузка состояния из SQLite
2. **Продолжение** - копирование с места остановки

## ✅ Результат

- Комментарии копируются правильно
- Альбомы отправляются целиком  
- Соблюдается хронологический порядок
- Данные сохраняются при перезапуске
- Поддерживаются все типы медиа

## 🔍 Мониторинг

```bash
# Проверка прогресса
sqlite3 telegram_copier_v3.db "SELECT COUNT(*) FROM posts WHERE processed = 1;"
sqlite3 telegram_copier_v3.db "SELECT COUNT(*) FROM comments WHERE processed = 1;"
```

## 🛑 Остановка

Используйте `Ctrl+C` для корректной остановки.