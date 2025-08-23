# Руководство по отладке альбомов в Telegram Copier

## Проблема

Альбомы из исходной группы (1-10 фото с общим описанием) копируются как отдельные сообщения вместо сохранения исходной группировки 1:1.

**Симптомы:**
- В логах: `Альбом успешно отправлен как 1 сообщений`
- В целевой группе: каждое фото из альбома появляется как отдельный пост
- Исходный альбом: 5 фото + текст → Результат: 5 отдельных постов

## Анализ проблемы

### Как работают альбомы в Telegram
1. **grouped_id** - все сообщения альбома имеют одинаковый идентификатор группы
2. **Лимит альбома** - максимум 10 медиа-файлов в одном альбоме
3. **API требования** - нужно отправлять альбом специальным образом для сохранения группировки

### Исследованные подходы в коде

Реализованы **3 варианта** отправки альбомов с fallback логикой:

#### Вариант 1: Передача сообщений напрямую
```python
send_kwargs = {
    'entity': self.target_entity,
    'file': album_messages,  # Передаем сами сообщения
    'caption': caption,
}
sent_messages = await self.client.send_file(**send_kwargs)
```

#### Вариант 2: Извлечение медиа-объектов
```python
media_files = [message.media for message in album_messages if message.media]
send_kwargs = {
    'entity': self.target_entity,
    'file': media_files,  # Массив медиа объектов
    'caption': caption,
}
sent_messages = await self.client.send_file(**send_kwargs)
```

#### Вариант 3: InputMedia с SendMultiMediaRequest
```python
input_media = []
for message in album_messages:
    if isinstance(message.media, MessageMediaPhoto):
        input_media.append(InputMediaPhoto(message.media.photo))
    elif isinstance(message.media, MessageMediaDocument):
        input_media.append(InputMediaDocument(message.media.document))

request = SendMultiMediaRequest(
    peer=self.target_entity,
    multi_media=input_media,
    message=caption or "",
    random_id=None
)
result = await self.client(request)
```

## Диагностика

### Логи для отладки
Добавлено детальное логирование:

```
DEBUG - Альбом содержит X медиа из Y сообщений
DEBUG - Пробуем отправить альбом как сообщения (вариант 1)
DEBUG - Отправляем альбом из X сообщений
INFO - ✅ Альбом успешно отправлен как Y сообщений
```

### Ключевые проверки
1. **Результат отправки:**
   - `isinstance(sent_messages, list)` - должен быть список для альбома
   - Если одно сообщение - группировка потеряна

2. **Количество медиа:**
   - Проверяем `sum(1 for msg in album_messages if msg.media)`
   - Пустые альбомы отправляются как текст

## Возможные причины проблемы

### 1. Telethon API особенности
- `send_file` может интерпретировать медиа по-разному
- Версия Telethon может не поддерживать групповую отправку
- Формат медиа-объектов может быть неподходящим

### 2. Медиа-типы
- Разные типы медиа (фото/видео/документы) в одном альбоме
- Поврежденные или недоступные медиа-файлы
- Проблемы с доступом к исходным медиа

### 3. API ограничения
- Права доступа в целевом канале
- Telegram API лимиты на группированную отправку
- Особенности конкретного аккаунта/бота

## Рекомендации по тестированию

### Шаг 1: Проверить версию Telethon
```bash
pip show telethon
```
Убедиться что версия поддерживает альбомы.

### Шаг 2: Тестировать на простом альбоме
1. Создать тестовый альбом из 2-3 фотографий
2. Запустить копирование в dry_run режиме
3. Проверить логи на предмет ошибок

### Шаг 3: Анализировать логи
Искать в логах:
- `DEBUG - Пробуем отправить альбом как...`
- `WARNING - Не удалось отправить альбом как...`
- `INFO - ✅ Альбом успешно отправлен как X сообщений`

### Шаг 4: Проверить результат
Если `X = 1` - проблема сохраняется, нужны дополнительные исправления.

## Альтернативные решения

### Решение 1: Использовать events.Album
Переписать логику с использованием событийной модели:
```python
@client.on(events.Album(chats=source_chat))
async def album_handler(event):
    await client.send_message(
        target_chat,
        file=event.messages,
        message=event.text,
    )
```

### Решение 2: Ручная группировка через Bot API
Если Telethon не справляется, использовать Bot API:
```python
# Отправка через sendMediaGroup
media_group = [
    InputMediaPhoto(media=photo_file_id),
    InputMediaPhoto(media=photo_file_id),
    # ...
]
```

### Решение 3: Форвардинг с группировкой
Попробовать форвардить альбом целиком:
```python
await client.forward_messages(
    entity=target,
    messages=album_messages,
    from_peer=source
)
```

## Следующие шаги

1. **Протестировать текущие исправления** на реальных альбомах
2. **Проанализировать логи** для определения какой вариант работает
3. **Если все варианты дают 1 сообщение** - рассмотреть альтернативные подходы
4. **Изучить исходный код Telethon** для понимания как правильно отправлять альбомы

## Дополнительные ресурсы

- [Документация Telethon](https://docs.telethon.dev/)
- [Примеры альбомов в Telethon](https://tl.telethon.dev/)
- [GitHub Issues по альбомам](https://github.com/LonamiWebs/Telethon/issues)

---

*Обновлено после реализации экспериментальных исправлений*