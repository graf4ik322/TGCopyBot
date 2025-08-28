#!/usr/bin/env python3
"""
Message Deletion Script for Telegram Groups
Safely deletes messages in a specified ID range with proper rate limiting.
Uses existing authentication module and follows Telegram API constraints.
"""

import asyncio
import sys
import signal
import argparse
import logging
import os
from typing import Optional, List
from telethon import TelegramClient
from telethon.errors import FloodWaitError, MessageDeleteForbiddenError, MessageIdInvalidError
from telethon.tl.types import Message

from config import Config
from utils import setup_logging, RateLimiter, handle_flood_wait


class MessageDeleter:
    """Class for batch deletion of Telegram messages with proper rate limiting."""
    
    def __init__(self, target_group_id: str, start_id: int, end_id: int, dry_run: bool = False):
        """
        Initialize the message deleter.
        
        Args:
            target_group_id: ID or username of the target group/channel
            start_id: Starting message ID (inclusive)
            end_id: Ending message ID (inclusive) 
            dry_run: If True, only simulate deletion without actually deleting
        """
        self.config = Config()
        self.logger = setup_logging()
        self.client: Optional[TelegramClient] = None
        self.target_group_id = target_group_id
        self.start_id = start_id
        self.end_id = end_id
        self.dry_run = dry_run
        self.running = False
        
        # Rate limiter for deletion operations (more conservative than copying)
        # Telegram allows ~20 messages per minute, we use 15 for safety
        self.rate_limiter = RateLimiter(
            messages_per_hour=900,  # 15 per minute = 900 per hour
            delay_seconds=4  # 4 seconds between deletions for safety
        )
        
        # Statistics
        self.deleted_count = 0
        self.failed_count = 0
        self.skipped_count = 0
        
        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    async def _create_client(self) -> bool:
        """Create and configure Telegram client."""
        try:
            # Use existing session configuration
            data_dir = '/app/data' if os.path.exists('/app/data') else '.'
            session_path = os.path.join(data_dir, self.config.session_name)
            
            # Get proxy configuration if available
            proxy_config = self.config.get_proxy_config()
            
            self.logger.info(f"Creating client with session: {session_path}")
            
            self.client = TelegramClient(
                session_path,
                self.config.api_id,
                self.config.api_hash,
                proxy=proxy_config,
                device_model="Telegram Message Deleter",
                app_version="1.0.0",
                system_version="Linux"
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating client: {e}")
            return False
    
    async def _authenticate(self) -> bool:
        """Authenticate with Telegram using existing session."""
        try:
            self.logger.info("Starting Telegram client...")
            await asyncio.wait_for(self.client.start(), timeout=30.0)
            
            # Check if we're authorized
            if await self.client.is_user_authorized():
                try:
                    me = await self.client.get_me()
                    if me and me.id:
                        self.logger.info(f"Authenticated as: {me.first_name} {me.last_name or ''} (@{me.username or 'no_username'})")
                        return True
                    else:
                        self.logger.error("Session exists but user data unavailable")
                        return False
                except Exception as e:
                    self.logger.error(f"Error verifying authentication: {e}")
                    return False
            else:
                self.logger.error("User not authorized. Please run the main copier script first to create a valid session.")
                return False
                
        except asyncio.TimeoutError:
            self.logger.error("Timeout starting client (30 sec). Possible session issue.")
            return False
        except Exception as e:
            self.logger.error(f"Error during authentication: {e}")
            return False
    
    async def _validate_group_access(self) -> bool:
        """Validate access to the target group and check deletion permissions."""
        try:
            self.logger.info(f"Validating access to group: {self.target_group_id}")
            
            # Get group entity
            target_entity = await self.client.get_entity(self.target_group_id)
            self.logger.info(f"Found group: {target_entity.title}")
            
            # Check if we have admin permissions (required for deleting messages)
            try:
                permissions = await self.client.get_permissions(target_entity)
                if permissions.is_admin or permissions.delete_messages:
                    self.logger.info("✅ Admin permissions confirmed - can delete messages")
                    return True
                else:
                    self.logger.warning("⚠️ No admin permissions detected - may only be able to delete own messages")
                    # Don't return False here, let the actual deletion attempt determine what we can delete
                    return True
            except Exception as e:
                self.logger.warning(f"Could not verify permissions: {e}")
                return True  # Proceed anyway, let deletion attempts determine what's possible
                
        except Exception as e:
            self.logger.error(f"Error validating group access: {e}")
            return False
    
    async def _delete_message_batch(self, message_ids: List[int]) -> int:
        """
        Delete a batch of messages.
        
        Args:
            message_ids: List of message IDs to delete
            
        Returns:
            Number of successfully deleted messages
        """
        if self.dry_run:
            self.logger.info(f"DRY RUN: Would delete messages: {message_ids}")
            return len(message_ids)
        
        try:
            # Use delete_messages for batch deletion (more efficient)
            deleted_messages = await self.client.delete_messages(
                self.target_group_id, 
                message_ids
            )
            
            # deleted_messages contains the actual deleted count
            actual_deleted = len(deleted_messages) if deleted_messages else 0
            
            if actual_deleted > 0:
                self.logger.info(f"✅ Deleted {actual_deleted} messages from batch: {message_ids}")
            else:
                self.logger.warning(f"⚠️ No messages deleted from batch: {message_ids}")
            
            return actual_deleted
            
        except MessageDeleteForbiddenError as e:
            self.logger.error(f"❌ Forbidden to delete messages {message_ids}: {e}")
            return 0
        except MessageIdInvalidError as e:
            self.logger.warning(f"⚠️ Invalid message IDs in batch {message_ids}: {e}")
            return 0
        except FloodWaitError as e:
            self.logger.warning(f"Rate limited, waiting {e.seconds} seconds...")
            await handle_flood_wait(e, self.logger, f"deleting batch {message_ids}")
            # Retry the batch after flood wait
            return await self._delete_message_batch(message_ids)
        except Exception as e:
            self.logger.error(f"❌ Error deleting batch {message_ids}: {e}")
            return 0
    
    async def delete_messages_in_range(self) -> bool:
        """
        Delete messages in the specified ID range.
        
        Returns:
            True if operation completed successfully
        """
        if not await self._create_client():
            return False
        
        if not await self._authenticate():
            return False
        
        if not await self._validate_group_access():
            return False
        
        total_messages = self.end_id - self.start_id + 1
        self.logger.info(f"🗑️ Starting deletion of messages {self.start_id} to {self.end_id} ({total_messages} total)")
        
        if self.dry_run:
            self.logger.info("🔍 DRY RUN MODE - No messages will be actually deleted")
        
        self.running = True
        
        # Process messages in batches for efficiency
        batch_size = 100  # Telegram allows up to 100 messages per delete_messages call
        current_id = self.start_id
        
        try:
            while current_id <= self.end_id and self.running:
                # Create batch of message IDs
                batch_end = min(current_id + batch_size - 1, self.end_id)
                batch_ids = list(range(current_id, batch_end + 1))
                
                self.logger.info(f"Processing batch: {current_id} to {batch_end}")
                
                # Apply rate limiting
                await self.rate_limiter.wait_if_needed()
                
                # Delete the batch
                deleted_in_batch = await self._delete_message_batch(batch_ids)
                
                # Update statistics
                self.deleted_count += deleted_in_batch
                failed_in_batch = len(batch_ids) - deleted_in_batch
                self.failed_count += failed_in_batch
                
                # Record the operation for rate limiting
                self.rate_limiter.record_message_sent()
                
                # Progress update
                processed = batch_end - self.start_id + 1
                progress = (processed / total_messages) * 100
                self.logger.info(f"Progress: {progress:.1f}% ({processed}/{total_messages})")
                
                current_id = batch_end + 1
            
            # Final statistics
            if self.running:
                self.logger.info("=" * 50)
                self.logger.info("🎯 DELETION COMPLETED")
                self.logger.info(f"✅ Successfully deleted: {self.deleted_count}")
                self.logger.info(f"❌ Failed to delete: {self.failed_count}")
                self.logger.info(f"📊 Total processed: {self.deleted_count + self.failed_count}")
                self.logger.info("=" * 50)
                return True
            else:
                self.logger.info("Operation cancelled by user")
                return False
                
        except Exception as e:
            self.logger.error(f"Fatal error during deletion: {e}")
            return False
        finally:
            if self.client:
                await self.client.disconnect()


async def main():
    """Main function to handle command line arguments and run deletion."""
    parser = argparse.ArgumentParser(description='Delete Telegram messages in a specified ID range')
    parser.add_argument('group_id', help='Target group ID or username')
    parser.add_argument('start_id', type=int, help='Starting message ID (inclusive)')
    parser.add_argument('end_id', type=int, help='Ending message ID (inclusive)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Simulate deletion without actually deleting messages')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_id < 1:
        print("Error: start_id must be >= 1")
        return False
    
    if args.end_id < args.start_id:
        print("Error: end_id must be >= start_id")
        return False
    
    if args.end_id - args.start_id > 50000:
        print("Warning: Large deletion range. Consider smaller batches for safety.")
        confirm = input("Continue? (y/N): ")
        if confirm.lower() != 'y':
            return False
    
    # Create and run deleter
    deleter = MessageDeleter(
        target_group_id=args.group_id,
        start_id=args.start_id,
        end_id=args.end_id,
        dry_run=args.dry_run
    )
    
    return await deleter.delete_messages_in_range()


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)