#!/usr/bin/env python3
"""
Group Information Utility
Helps find the correct group ID and check permissions.
"""

import asyncio
import sys
import os
from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

from config import Config
from utils import setup_logging


async def get_group_info():
    """Get information about groups the user has access to."""
    config = Config()
    
    if not config.validate():
        print("❌ Invalid configuration. Please check your .env file.")
        return False

    logger = setup_logging(config.log_level)
    
    # Create client
    data_dir = '/app/data' if os.path.exists('/app/data') else '.'
    session_path = os.path.join(data_dir, config.session_name)
    
    client = TelegramClient(
        session_path,
        config.api_id,
        config.api_hash,
        proxy=config.get_proxy_config()
    )
    
    try:
        print("🔍 Getting group information...")
        await client.start()
        
        print("\n📋 Available Groups and Channels:")
        print("=" * 60)
        
        found_groups = 0
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            
            # Only show groups and channels
            if isinstance(entity, (Channel, Chat)):
                found_groups += 1
                group_type = "Channel" if isinstance(entity, Channel) and entity.broadcast else "Group"
                privacy = "Private" if hasattr(entity, 'access_hash') and entity.access_hash else "Public"
                
                print(f"\n📁 {group_type} ({privacy}): {entity.title}")
                print(f"   ID: {entity.id}")
                print(f"   Username: @{entity.username}" if entity.username else "   Username: None")
                
                # Show different ID formats for testing
                print(f"   For .env file: {entity.id}")
                if entity.id < 0:
                    print(f"   Alternative: {entity.id}")
                else:
                    print(f"   Alternative: -{entity.id}")
                
                # Show if it's private
                if not entity.username:
                    print("   🔒 Private group - use numeric ID")
                
                # Check permissions
                try:
                    permissions = await client.get_permissions(entity)
                    if permissions.is_admin:
                        print("   ✅ Admin permissions - can delete any messages")
                    elif permissions.delete_messages:
                        print("   ✅ Delete permissions - can delete messages")
                    else:
                        print("   ⚠️  No delete permissions - can only delete own messages")
                except Exception as e:
                    print(f"   ❓ Could not check permissions: {e}")
        
        if found_groups == 0:
            print("❌ No groups or channels found. Make sure you're a member of some groups.")
        
        # If user provided a specific group ID to check
        if len(sys.argv) > 1:
            test_id = sys.argv[1]
            print(f"\n🔎 Testing specific group ID: {test_id}")
            print("-" * 40)
            
            try:
                entity = await client.get_entity(test_id)
                print(f"✅ Successfully found: {entity.title}")
                print(f"   Type: {'Channel' if isinstance(entity, Channel) and entity.broadcast else 'Group'}")
                print(f"   ID: {entity.id}")
                print(f"   Username: @{entity.username}" if entity.username else "   Username: None")
                
                # Check permissions
                try:
                    permissions = await client.get_permissions(entity)
                    if permissions.is_admin:
                        print("   ✅ Admin permissions - can delete any messages")
                    elif permissions.delete_messages:
                        print("   ✅ Delete permissions - can delete messages")
                    else:
                        print("   ⚠️  No delete permissions - can only delete own messages")
                except Exception as e:
                    print(f"   ❓ Could not check permissions: {e}")
                    
            except Exception as e:
                print(f"❌ Error accessing group {test_id}: {e}")
                print("\nSuggestions:")
                print("1. Make sure the bot is a member of the group")
                print("2. Try using @username instead of numeric ID")
                print("3. Check if the group exists and is accessible")
        
        print(f"\n💡 Usage tips:")
        print("1. Use @username format when possible (e.g., @my_group)")
        print("2. For numeric IDs, try different formats if one doesn't work")
        print("3. Make sure your bot/account is a member of the group")
        print("4. Admin permissions are required to delete other users' messages")
        
        return True
        
    except Exception as e:
        logger.error(f"Error getting group info: {e}")
        return False
    finally:
        await client.disconnect()


if __name__ == "__main__":
    import os
    
    print("🔍 Group Information Utility")
    print("=" * 40)
    
    if len(sys.argv) > 1:
        print(f"Testing group ID: {sys.argv[1]}")
    else:
        print("Showing all available groups")
        print("Usage: python3 get_group_info.py <group_id> (optional)")
    
    print()
    
    try:
        success = asyncio.run(get_group_info())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)