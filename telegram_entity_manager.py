"""
Telegram Entity Manager - Улучшенная система управления entities с retry механизмом
Решает проблему "Cannot find any entity corresponding to" через комплексный подход.
"""

import asyncio
import logging
import time
from typing import Optional, Union, List, Dict, Any
from telethon import TelegramClient
from telethon.tl.types import (
    Channel, Chat, User, PeerChannel, PeerChat, PeerUser,
    InputPeerChannel, InputPeerChat, InputPeerUser
)
from telethon.tl import functions
from telethon.errors import (
    FloodWaitError, PeerFloodError, ChannelPrivateError, 
    ChatAdminRequiredError, UserNotParticipantError,
    ChannelInvalidError, ValueError
)


class EntityNotFoundError(Exception):
    """Исключение для случаев, когда entity не может быть найдена."""
    pass


class EntityAccessDeniedError(Exception):
    """Исключение для случаев, когда нет доступа к entity."""
    pass


class TelegramEntityManager:
    """
    Продвинутый менеджер для работы с Telegram entities.
    Включает retry механизмы, кэширование и альтернативные методы поиска.
    """
    
    def __init__(self, client: TelegramClient, max_retries: int = 5):
        """
        Инициализация менеджера entities.
        
        Args:
            client: Авторизованный Telegram клиент
            max_retries: Максимальное количество попыток
        """
        self.client = client
        self.max_retries = max_retries
        self.logger = logging.getLogger('entity_manager')
        
        # Кэш для entities
        self._entity_cache: Dict[str, Any] = {}
        self._access_cache: Dict[str, bool] = {}
        
        # Статистика попыток
        self._attempt_stats: Dict[str, int] = {}
    
    async def get_entity_robust(self, 
                               entity_id: Union[str, int],
                               force_refresh: bool = False) -> Optional[Any]:
        """
        Получение entity с использованием множественных стратегий и retry механизма.
        
        Args:
            entity_id: ID или username entity
            force_refresh: Принудительное обновление кэша
            
        Returns:
            Entity объект или None если не найден
            
        Raises:
            EntityNotFoundError: Если entity не найдена после всех попыток
            EntityAccessDeniedError: Если нет доступа к entity
        """
        entity_key = str(entity_id)
        
        # Проверка кэша
        if not force_refresh and entity_key in self._entity_cache:
            self.logger.debug(f"Возврат entity {entity_key} из кэша")
            return self._entity_cache[entity_key]
        
        self.logger.info(f"🔍 Поиск entity: {entity_key}")
        
        # Множественные стратегии поиска
        strategies = [
            self._get_entity_direct,
            self._get_entity_with_dialogs_sync,
            self._get_entity_by_search,
            self._get_entity_by_username_variants,
            self._get_entity_by_invite_link
        ]
        
        last_exception = None
        
        for attempt in range(self.max_retries):
            self.logger.info(f"Попытка {attempt + 1}/{self.max_retries}")
            
            for strategy_idx, strategy in enumerate(strategies):
                try:
                    self.logger.debug(f"Стратегия {strategy_idx + 1}: {strategy.__name__}")
                    
                    entity = await strategy(entity_id)
                    
                    if entity:
                        # Проверка доступа
                        if await self._verify_entity_access(entity):
                            self._entity_cache[entity_key] = entity
                            self._access_cache[entity_key] = True
                            self.logger.info(f"✅ Entity {entity_key} найдена (стратегия {strategy_idx + 1})")
                            return entity
                        else:
                            self.logger.warning(f"⚠️ Нет доступа к entity {entity_key}")
                            self._access_cache[entity_key] = False
                            raise EntityAccessDeniedError(f"Нет доступа к {entity_key}")
                    
                except (FloodWaitError, PeerFloodError) as e:
                    wait_time = getattr(e, 'seconds', 30)
                    self.logger.warning(f"FloodWait: ожидание {wait_time}с")
                    await asyncio.sleep(wait_time)
                    
                except (ChannelPrivateError, ChatAdminRequiredError, UserNotParticipantError):
                    self.logger.warning(f"Нет доступа к {entity_key}")
                    self._access_cache[entity_key] = False
                    raise EntityAccessDeniedError(f"Нет доступа к {entity_key}")
                    
                except Exception as e:
                    last_exception = e
                    self.logger.debug(f"Стратегия {strategy_idx + 1} не сработала: {e}")
                    continue
            
            # Пауза между попытками
            if attempt < self.max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
        
        # Все стратегии исчерпаны
        self._attempt_stats[entity_key] = self._attempt_stats.get(entity_key, 0) + 1
        error_msg = f"Entity {entity_key} не найдена после {self.max_retries} попыток"
        
        if last_exception:
            error_msg += f". Последняя ошибка: {last_exception}"
        
        raise EntityNotFoundError(error_msg)
    
    async def _get_entity_direct(self, entity_id: Union[str, int]) -> Optional[Any]:
        """Прямое получение entity через get_entity."""
        return await self.client.get_entity(entity_id)
    
    async def _get_entity_with_dialogs_sync(self, entity_id: Union[str, int]) -> Optional[Any]:
        """Получение entity через синхронизацию диалогов."""
        try:
            # Принудительная синхронизация диалогов
            self.logger.debug("Синхронизация диалогов...")
            dialogs = await self.client.get_dialogs(limit=200)
            
            # Поиск в диалогах
            for dialog in dialogs:
                if self._match_entity(dialog.entity, entity_id):
                    return dialog.entity
                    
            # Повторная попытка get_entity после синхронизации
            return await self.client.get_entity(entity_id)
            
        except Exception as e:
            self.logger.debug(f"Синхронизация диалогов не помогла: {e}")
            return None
    
    async def _get_entity_by_search(self, entity_id: Union[str, int]) -> Optional[Any]:
        """Поиск entity через глобальный поиск."""
        try:
            if isinstance(entity_id, str) and entity_id.startswith('@'):
                username = entity_id[1:]  # Убираем @
                
                # Поиск через API
                result = await self.client(functions.contacts.SearchRequest(
                    q=username,
                    limit=10
                ))
                
                # Проверяем результаты
                for chat in result.chats:
                    if hasattr(chat, 'username') and chat.username == username:
                        return chat
                        
                for user in result.users:
                    if hasattr(user, 'username') and user.username == username:
                        return user
                        
        except Exception as e:
            self.logger.debug(f"Поиск через SearchRequest не удался: {e}")
            
        return None
    
    async def _get_entity_by_username_variants(self, entity_id: Union[str, int]) -> Optional[Any]:
        """Поиск entity через варианты username."""
        if not isinstance(entity_id, str):
            return None
            
        variants = []
        
        if entity_id.startswith('@'):
            username = entity_id[1:]
            variants = [entity_id, username, f"t.me/{username}"]
        else:
            variants = [entity_id, f"@{entity_id}", f"t.me/{entity_id}"]
            
        for variant in variants:
            try:
                entity = await self.client.get_entity(variant)
                if entity:
                    return entity
            except:
                continue
                
        return None
    
    async def _get_entity_by_invite_link(self, entity_id: Union[str, int]) -> Optional[Any]:
        """Попытка получения через invite link (если это ссылка)."""
        if not isinstance(entity_id, str) or not entity_id.startswith('https://t.me/'):
            return None
            
        try:
            # Извлекаем username из ссылки
            parts = entity_id.split('/')
            if len(parts) >= 4:
                username = parts[-1]
                return await self.client.get_entity(f"@{username}")
        except:
            pass
            
        return None
    
    def _match_entity(self, entity: Any, target_id: Union[str, int]) -> bool:
        """Проверка соответствия entity целевому ID."""
        try:
            # По ID
            if isinstance(target_id, int) and hasattr(entity, 'id'):
                return entity.id == target_id
                
            # По username
            if isinstance(target_id, str):
                if target_id.startswith('@'):
                    username = target_id[1:]
                else:
                    username = target_id
                    
                if hasattr(entity, 'username') and entity.username:
                    return entity.username.lower() == username.lower()
                    
                # Проверка title для каналов без username
                if hasattr(entity, 'title') and entity.title:
                    return username.lower() in entity.title.lower()
                    
        except Exception:
            pass
            
        return False
    
    async def _verify_entity_access(self, entity: Any) -> bool:
        """Проверка доступа к entity."""
        try:
            # Для каналов проверяем права
            if hasattr(entity, 'id') and str(entity.id).startswith('-100'):
                # Пытаемся получить информацию о канале
                await self.client(functions.channels.GetFullChannelRequest(entity))
                return True
                
            # Для обычных чатов
            elif hasattr(entity, 'id'):
                # Пытаемся получить базовую информацию
                await self.client.get_entity(entity.id)
                return True
                
        except (ChannelPrivateError, ChatAdminRequiredError, UserNotParticipantError):
            return False
        except Exception as e:
            self.logger.debug(f"Ошибка проверки доступа: {e}")
            return False
            
        return True
    
    async def get_entity_info(self, entity_id: Union[str, int]) -> Dict[str, Any]:
        """
        Получение подробной информации об entity.
        
        Returns:
            Словарь с информацией об entity
        """
        try:
            entity = await self.get_entity_robust(entity_id)
            
            info = {
                'id': getattr(entity, 'id', None),
                'title': getattr(entity, 'title', None),
                'username': getattr(entity, 'username', None),
                'type': entity.__class__.__name__,
                'access': True
            }
            
            # Дополнительная информация для каналов
            if hasattr(entity, 'id') and str(entity.id).startswith('-100'):
                try:
                    full_info = await self.client(functions.channels.GetFullChannelRequest(entity))
                    info.update({
                        'participants_count': getattr(full_info.full_chat, 'participants_count', None),
                        'about': getattr(full_info.full_chat, 'about', None),
                        'linked_chat_id': getattr(full_info.full_chat, 'linked_chat_id', None)
                    })
                except:
                    pass
                    
            return info
            
        except EntityNotFoundError:
            return {'id': entity_id, 'access': False, 'error': 'Entity not found'}
        except EntityAccessDeniedError:
            return {'id': entity_id, 'access': False, 'error': 'Access denied'}
        except Exception as e:
            return {'id': entity_id, 'access': False, 'error': str(e)}
    
    def clear_cache(self):
        """Очистка кэша entities."""
        self._entity_cache.clear()
        self._access_cache.clear()
        self.logger.info("Кэш entities очищен")
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики работы менеджера."""
        return {
            'cached_entities': len(self._entity_cache),
            'access_cache_size': len(self._access_cache),
            'attempt_stats': self._attempt_stats.copy()
        }


async def test_entity_manager():
    """Тестовая функция для проверки работы менеджера."""
    # Пример использования
    from telethon import TelegramClient
    
    client = TelegramClient('test_session', 'api_id', 'api_hash')
    manager = TelegramEntityManager(client)
    
    await client.start()
    
    try:
        # Тест с проблемным ID
        entity_info = await manager.get_entity_info('-1002399927446')
        print(f"Результат: {entity_info}")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        
    finally:
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(test_entity_manager())