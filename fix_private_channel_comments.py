#!/usr/bin/env python3
"""
Исправления для корректной работы с комментариями в приватных каналах.
Патчит существующий copier.py для решения проблем с загрузкой и копированием комментариев.
"""

import asyncio
import logging
from typing import List, Dict, Any, Set
from telethon.tl.types import Message, PeerChannel
from telethon.tl.functions.channels import GetFullChannelRequest


def patch_copier_for_private_channels(copier_instance):
    """
    Патчит экземпляр TelegramCopier для корректной работы с приватными каналами.
    
    Args:
        copier_instance: Экземпляр TelegramCopier для патчинга
    """
    logger = logging.getLogger('private_channel_patch')
    logger.info("🔧 Применяем патч для приватных каналов...")
    
    # Сохраняем оригинальные методы
    original_preload_comments = copier_instance.preload_all_comments_cache
    original_get_comments_from_cache = copier_instance.get_comments_from_cache
    original_find_discussion_groups = getattr(copier_instance, '_find_discussion_groups', None)
    
    async def enhanced_find_discussion_groups(source_entity) -> Set[int]:
        """
        Улучшенный поиск discussion groups для приватных каналов.
        
        Args:
            source_entity: Сущность исходного канала
            
        Returns:
            Множество ID найденных discussion groups
        """
        discussion_groups = set()
        
        try:
            # Метод 1: Получаем linked_chat_id из полной информации канала
            try:
                full_channel = await copier_instance.client(GetFullChannelRequest(source_entity))
                if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                    linked_chat_id = full_channel.full_chat.linked_chat_id
                    discussion_groups.add(linked_chat_id)
                    logger.info(f"✅ Найдена связанная группа через API: {linked_chat_id}")
            except Exception as e:
                logger.warning(f"⚠️ Не удалось получить linked_chat_id: {e}")
            
            # Метод 2: Проверяем настройки конфигурации
            if hasattr(copier_instance.config, 'discussion_group_id') and copier_instance.config.discussion_group_id:
                try:
                    discussion_group_id = int(copier_instance.config.discussion_group_id)
                    discussion_groups.add(discussion_group_id)
                    logger.info(f"✅ Используем группу из конфигурации: {discussion_group_id}")
                except ValueError:
                    # Возможно, это username
                    try:
                        entity = await copier_instance.client.get_entity(copier_instance.config.discussion_group_id)
                        discussion_groups.add(entity.id)
                        logger.info(f"✅ Получена группа по username: {entity.id}")
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось получить группу по username: {e}")
            
            # Метод 3: Сканирование сообщений канала (как в оригинальном коде)
            scanned_count = 0
            async for message in copier_instance.client.iter_messages(source_entity, limit=1000):
                scanned_count += 1
                
                if (hasattr(message, 'replies') and message.replies and
                    hasattr(message.replies, 'comments') and message.replies.comments and
                    hasattr(message.replies, 'channel_id') and message.replies.channel_id):
                    discussion_groups.add(message.replies.channel_id)
                
                if scanned_count % 100 == 0:
                    logger.info(f"   📊 Просканировано {scanned_count} сообщений...")
            
            logger.info(f"📊 Поиск discussion groups завершен: найдено {len(discussion_groups)} групп")
            for group_id in discussion_groups:
                logger.info(f"   📋 Discussion group: {group_id}")
            
            return discussion_groups
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска discussion groups: {e}")
            return set()
    
    async def enhanced_preload_comments(sample_batch: List[Message]) -> None:
        """
        Улучшенная предварительная загрузка комментариев для приватных каналов.
        
        Args:
            sample_batch: Первый батч сообщений
        """
        if copier_instance.comments_cache_loaded and not copier_instance.force_reload_comments:
            return
        
        try:
            logger.info("🔄 УЛУЧШЕННАЯ ПРЕДЗАГРУЗКА: Поиск discussion groups для приватного канала...")
            
            # Используем улучшенный поиск discussion groups
            discussion_groups = await enhanced_find_discussion_groups(copier_instance.source_entity)
            
            if not discussion_groups:
                logger.warning("⚠️ Discussion groups не найдены для приватного канала")
                copier_instance.comments_cache_loaded = True
                copier_instance.force_reload_comments = False
                return
            
            # Загружаем комментарии из всех найденных групп
            total_comments = 0
            total_posts_with_comments = 0
            
            for discussion_group_id in discussion_groups:
                logger.info(f"📥 Загружаем комментарии из приватной группы {discussion_group_id}")
                
                try:
                    # Используем улучшенный метод загрузки
                    comments_by_post = await enhanced_get_comments_from_discussion_group(
                        copier_instance, discussion_group_id
                    )
                    
                    # Добавляем в кэш
                    for post_id, comments in comments_by_post.items():
                        if post_id not in copier_instance.comments_cache:
                            copier_instance.comments_cache[post_id] = []
                            total_posts_with_comments += 1
                        copier_instance.comments_cache[post_id].extend(comments)
                        total_comments += len(comments)
                    
                    logger.info(f"✅ Загружено {len(comments_by_post)} связей из группы {discussion_group_id}")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка загрузки из группы {discussion_group_id}: {e}")
            
            copier_instance.comments_cache_loaded = True
            copier_instance.force_reload_comments = False
            
            logger.info(f"🎯 УЛУЧШЕННАЯ ПРЕДЗАГРУЗКА ЗАВЕРШЕНА:")
            logger.info(f"   📊 Загружено {total_comments} комментариев")
            logger.info(f"   📊 Для {total_posts_with_comments} постов")
            logger.info(f"   📊 Из {len(discussion_groups)} discussion groups")
            
        except Exception as e:
            logger.error(f"❌ Ошибка улучшенной предзагрузки: {e}")
            copier_instance.comments_cache_loaded = True
            copier_instance.force_reload_comments = False
    
    async def enhanced_get_comments_from_discussion_group(copier_instance, discussion_group_id: int) -> Dict[int, List[Message]]:
        """
        Улучшенное получение комментариев из discussion group для приватных каналов.
        
        Args:
            copier_instance: Экземпляр копировщика
            discussion_group_id: ID discussion group
            
        Returns:
            Словарь {channel_post_id: [comments]}
        """
        comments_by_post = {}
        
        try:
            discussion_group = PeerChannel(discussion_group_id)
            logger.info(f"🔍 Сканируем приватную группу обсуждений {discussion_group_id}")
            
            message_count = 0
            forwarded_posts = {}  # Карта пересланных постов
            
            # ЭТАП 1: Находим все переслянные посты из канала
            async for disc_message in copier_instance.client.iter_messages(discussion_group, limit=None):
                message_count += 1
                
                if message_count % 500 == 0:
                    logger.info(f"   📊 Обработано {message_count} сообщений из группы...")
                
                # Проверяем пересланные сообщения
                if (hasattr(disc_message, 'fwd_from') and disc_message.fwd_from and
                    hasattr(disc_message.fwd_from, 'channel_post') and disc_message.fwd_from.channel_post):
                    
                    channel_post_id = disc_message.fwd_from.channel_post
                    forwarded_posts[channel_post_id] = disc_message.id
                    logger.debug(f"📤 Найден пересланный пост: канал {channel_post_id} -> группа {disc_message.id}")
                
                # Проверяем прямые связи (для некоторых приватных каналов)
                elif hasattr(disc_message, 'id') and not hasattr(disc_message, 'reply_to'):
                    # Это может быть автоматически созданный пост для комментариев
                    # Пробуем связать по ID (часто ID совпадают в приватных каналах)
                    try:
                        # Проверяем, есть ли сообщение с таким ID в канале
                        test_message = await copier_instance.client.get_messages(
                            copier_instance.source_entity, ids=disc_message.id
                        )
                        if test_message and not test_message.empty:
                            forwarded_posts[disc_message.id] = disc_message.id
                            logger.debug(f"🔗 Прямая связь: канал {disc_message.id} -> группа {disc_message.id}")
                    except Exception:
                        pass  # Игнорируем ошибки
            
            logger.info(f"📊 Найдено {len(forwarded_posts)} связанных постов в группе")
            
            # ЭТАП 2: Собираем комментарии к каждому посту
            for channel_post_id, discussion_message_id in forwarded_posts.items():
                try:
                    comments = []
                    comment_count = 0
                    
                    # Получаем все ответы на пересланное сообщение
                    async for comment in copier_instance.client.iter_messages(
                        discussion_group,
                        reply_to=discussion_message_id,
                        limit=None
                    ):
                        comments.append(comment)
                        comment_count += 1
                    
                    if comments:
                        comments_by_post[channel_post_id] = comments
                        logger.debug(f"💬 Пост {channel_post_id}: собрано {comment_count} комментариев")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сбора комментариев для поста {channel_post_id}: {e}")
            
            logger.info(f"✅ Обработка группы завершена: {len(comments_by_post)} постов с комментариями")
            
        except Exception as e:
            logger.error(f"❌ Ошибка обработки discussion group {discussion_group_id}: {e}")
        
        return comments_by_post
    
    async def enhanced_get_comments_from_cache(message: Message) -> List[Message]:
        """
        Улучшенное получение комментариев из кэша с дополнительными проверками.
        
        Args:
            message: Сообщение канала
            
        Returns:
            Список комментариев
        """
        try:
            # Сначала пробуем оригинальный метод
            comments = copier_instance.comments_cache.get(message.id, [])
            
            if comments:
                logger.debug(f"💬 Сообщение {message.id}: найдено {len(comments)} комментариев в кэше")
                return comments
            
            # Если не найдено, проверяем, загружен ли кэш
            if not copier_instance.comments_cache_loaded:
                logger.warning(f"⚠️ Кэш комментариев не загружен для сообщения {message.id}")
                # Принудительно загружаем кэш
                copier_instance.force_reload_comments = True
                await enhanced_preload_comments([message])
                
                # Повторная попытка
                comments = copier_instance.comments_cache.get(message.id, [])
                if comments:
                    logger.debug(f"💬 После перезагрузки - сообщение {message.id}: найдено {len(comments)} комментариев")
                    return comments
            
            logger.debug(f"💬 Сообщение {message.id}: комментариев не найдено")
            return []
            
        except Exception as e:
            logger.warning(f"Ошибка получения комментариев для сообщения {message.id}: {e}")
            return []
    
    # Применяем патчи
    copier_instance.preload_all_comments_cache = enhanced_preload_comments
    copier_instance.get_comments_from_cache = enhanced_get_comments_from_cache
    copier_instance._enhanced_find_discussion_groups = enhanced_find_discussion_groups
    
    # Добавляем метод для диагностики
    async def diagnose_comments_setup():
        """Диагностика настройки комментариев для приватного канала."""
        logger.info("🔍 Диагностика настройки комментариев...")
        
        # Проверяем конфигурацию
        if hasattr(copier_instance, 'config'):
            config = copier_instance.config
            logger.info(f"   📋 Source group: {config.source_group_id}")
            logger.info(f"   📋 Target group: {config.target_group_id}")
            
            if hasattr(config, 'discussion_group_id') and config.discussion_group_id:
                logger.info(f"   📋 Discussion group: {config.discussion_group_id}")
            else:
                logger.warning("   ⚠️ Discussion group ID не указан в конфигурации")
        
        # Проверяем состояние кэша
        cache_size = len(copier_instance.comments_cache)
        logger.info(f"   📊 Размер кэша комментариев: {cache_size}")
        logger.info(f"   📊 Кэш загружен: {copier_instance.comments_cache_loaded}")
        logger.info(f"   📊 Принудительная перезагрузка: {copier_instance.force_reload_comments}")
        
        if cache_size > 0:
            logger.info("   ✅ Комментарии найдены в кэше")
            # Показываем примеры
            for post_id, comments in list(copier_instance.comments_cache.items())[:3]:
                logger.info(f"      - Пост {post_id}: {len(comments)} комментариев")
        else:
            logger.warning("   ⚠️ Кэш комментариев пуст")
    
    copier_instance.diagnose_comments_setup = diagnose_comments_setup
    
    logger.info("✅ Патч для приватных каналов применен")
    logger.info("   - Улучшен поиск discussion groups")
    logger.info("   - Добавлена поддержка конфигурации discussion_group_id")
    logger.info("   - Улучшена загрузка комментариев")
    logger.info("   - Добавлены дополнительные проверки")
    logger.info("   - Добавлен метод диагностики: diagnose_comments_setup()")


# Функция для применения патча к существующему экземпляру
def apply_private_channel_patch(copier_instance):
    """
    Применяет патч к существующему экземпляру копировщика.
    
    Args:
        copier_instance: Экземпляр TelegramCopier
    """
    patch_copier_for_private_channels(copier_instance)
    return copier_instance


if __name__ == "__main__":
    print("📋 Модуль патчей для приватных каналов")
    print("Используйте apply_private_channel_patch(copier_instance) для применения патча")