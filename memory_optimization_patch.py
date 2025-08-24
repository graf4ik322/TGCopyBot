"""
Патч для интеграции оптимизированного кэширования в TelegramCopier.
Применяет минимальные изменения к существующему коду.
"""

from optimized_cache import OptimizedCommentsStorage
import logging


def apply_memory_optimization(copier_instance, memory_limit_mb=100):
    """
    Применить оптимизацию памяти к существующему экземпляру TelegramCopier.
    
    Args:
        copier_instance: Экземпляр TelegramCopier
        memory_limit_mb: Лимит памяти в мегабайтах (по умолчанию 100 MB)
    """
    logger = logging.getLogger('telegram_copier.memory_optimization')
    logger.info(f"🚀 Применяем оптимизацию памяти (лимит: {memory_limit_mb} MB)")
    
    # Создаем оптимизированное хранилище
    copier_instance.optimized_storage = OptimizedCommentsStorage(
        memory_limit_mb=memory_limit_mb,
        lru_cache_size=min(1000, memory_limit_mb * 10)  # 10 постов на MB
    )
    
    # Сохраняем ссылки на оригинальные методы
    copier_instance._original_get_comments_from_cache = copier_instance.get_comments_from_cache
    copier_instance._original_preload_all_comments_cache = copier_instance.preload_all_comments_cache
    
    # Заменяем методы на оптимизированные
    async def optimized_get_comments_from_cache(message):
        """Оптимизированное получение комментариев."""
        # Получаем MessageProxy объекты из оптимизированного хранилища
        # Они совместимы с оригинальными Message объектами по API
        message_proxies = await copier_instance.optimized_storage.get_comments_for_post(message.id)
        
        # MessageProxy объекты уже совместимы с оригинальным кодом
        return message_proxies
    
    async def optimized_preload_all_comments_cache(sample_batch):
        """Оптимизированная предзагрузка комментариев."""
        if copier_instance.comments_cache_loaded:
            return
        
        logger.info("🔄 ОПТИМИЗИРОВАННАЯ ПРЕДЗАГРУЗКА: Сканируем канал для поиска discussion groups...")
        
        # Сканируем канал для поиска discussion groups (уменьшенный лимит для экономии памяти)
        discussion_groups = set()
        scanned_messages = 0
        
        # Уменьшаем лимит сканирования для экономии памяти
        scan_limit = min(2000, memory_limit_mb * 20)  # 20 сообщений на MB памяти
        
        async for message in copier_instance.client.iter_messages(
            copier_instance.source_entity, 
            limit=scan_limit
        ):
            scanned_messages += 1
            
            if (hasattr(message, 'replies') and message.replies and
                hasattr(message.replies, 'comments') and message.replies.comments and
                hasattr(message.replies, 'channel_id') and message.replies.channel_id):
                discussion_groups.add(message.replies.channel_id)
            
            # Логируем прогресс каждые 500 сообщений
            if scanned_messages % 500 == 0:
                logger.info(f"   📊 Просканировано {scanned_messages} сообщений, "
                           f"найдено {len(discussion_groups)} discussion groups")
        
        logger.info(f"📊 СКАНИРОВАНИЕ ЗАВЕРШЕНО: просканировано {scanned_messages} сообщений, "
                   f"найдено {len(discussion_groups)} уникальных discussion groups")
        
        if not discussion_groups:
            logger.info("💬 Discussion groups не найдены в канале")
            copier_instance.comments_cache_loaded = True
            return
        
        # Загружаем комментарии батчами для экономии памяти
        total_comments = 0
        total_posts_with_comments = 0
        
        for discussion_group_id in discussion_groups:
            logger.info(f"📥 Загружаем комментарии из discussion group {discussion_group_id}")
            
            try:
                # Используем оригинальный метод для получения комментариев
                comments_by_post = await copier_instance.get_all_comments_from_discussion_group(discussion_group_id)
                
                # Сохраняем в оптимизированное хранилище
                await copier_instance.optimized_storage.store_comments_batch(comments_by_post)
                
                total_comments += sum(len(comments) for comments in comments_by_post.values())
                total_posts_with_comments += len(comments_by_post)
                
                logger.info(f"✅ Загружено {len(comments_by_post)} связей пост->комментарии из группы {discussion_group_id}")
                
                # Проверяем память после каждой группы
                await copier_instance.optimized_storage._check_memory_usage()
                
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки комментариев из группы {discussion_group_id}: {e}")
        
        copier_instance.comments_cache_loaded = True
        logger.info(f"🎯 ОПТИМИЗИРОВАННАЯ ПРЕДЗАГРУЗКА ЗАВЕРШЕНА:")
        logger.info(f"   📊 Обработано {total_comments} комментариев")
        logger.info(f"   📊 Для {total_posts_with_comments} постов с комментариями")
        logger.info(f"   💾 Данные сохранены в базе данных (экономия памяти: ~{total_comments * 0.001:.1f} MB)")
    
    # Заменяем методы
    copier_instance.get_comments_from_cache = optimized_get_comments_from_cache
    copier_instance.preload_all_comments_cache = optimized_preload_all_comments_cache
    
    # Обнуляем старые кэши для освобождения памяти
    if hasattr(copier_instance, 'comments_cache'):
        old_cache_size = len(copier_instance.comments_cache)
        copier_instance.comments_cache.clear()
        logger.info(f"🧹 Очищен старый кэш комментариев ({old_cache_size} элементов)")
    
    # Добавляем метод для получения статистики
    def get_memory_optimization_stats():
        """Получить статистику оптимизации памяти."""
        performance_stats = copier_instance.optimized_storage.get_performance_stats()
        memory_stats = copier_instance.optimized_storage.get_memory_stats()
        
        return {
            'cache_hit_ratio': performance_stats['cache_hit_ratio'],
            'total_memory_mb': memory_stats.total_memory_mb,
            'database_size_mb': memory_stats.database_size_mb,
            'cache_size': memory_stats.cache_size,
            'memory_cleanups': performance_stats['memory_cleanups'],
            'efficiency_ratio': memory_stats.efficiency_ratio
        }
    
    copier_instance.get_memory_optimization_stats = get_memory_optimization_stats
    
    # Добавляем cleanup в оригинальный метод
    original_cleanup = getattr(copier_instance, 'cleanup_temp_files', lambda: None)
    
    def enhanced_cleanup():
        """Расширенная очистка с оптимизацией памяти."""
        # Выполняем оригинальную очистку
        original_cleanup()
        
        # Дополнительная очистка оптимизированного хранилища
        if hasattr(copier_instance, 'optimized_storage'):
            copier_instance.optimized_storage.cleanup_old_data(days_old=7)
            
            # Показываем финальную статистику
            stats = get_memory_optimization_stats()
            logger.info(f"📊 ФИНАЛЬНАЯ СТАТИСТИКА ОПТИМИЗАЦИИ ПАМЯТИ:")
            logger.info(f"   💨 Cache hit ratio: {stats['cache_hit_ratio']:.1%}")
            logger.info(f"   💾 Использование памяти: {stats['total_memory_mb']:.1f} MB")
            logger.info(f"   🗄️ Размер базы данных: {stats['database_size_mb']:.1f} MB")
            logger.info(f"   🧹 Очисток памяти: {stats['memory_cleanups']}")
            
            copier_instance.optimized_storage.close()
    
    copier_instance.cleanup_temp_files = enhanced_cleanup
    
    logger.info("✅ Оптимизация памяти применена успешно")


# Функция для быстрого применения оптимизации к существующему коду
def quick_optimize_memory(copier, memory_limit_mb=50):
    """
    Быстрая оптимизация памяти для слабых машин.
    
    Args:
        copier: Экземпляр TelegramCopier
        memory_limit_mb: Лимит памяти (по умолчанию 50 MB для слабых машин)
    """
    apply_memory_optimization(copier, memory_limit_mb)