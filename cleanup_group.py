#!/usr/bin/env python3
"""
Group Cleanup Script
Quick cleanup script for removing development test messages.
Deletes messages with IDs 1 to 17870 from the target group.
"""

import asyncio
import sys
from message_deleter import MessageDeleter
from config import Config


async def cleanup_development_messages():
    """Clean up development test messages from the target group."""
    print("🧹 Group Cleanup Script")
    print("=" * 40)
    
    # Load configuration to get target group
    config = Config()
    
    if not config.validate():
        print("❌ Invalid configuration. Please check your .env file.")
        return False
    
    target_group = config.target_group_id
    start_id = 1
    end_id = 17870
    total_messages = end_id - start_id + 1
    estimated_batches = (total_messages + 99) // 100  # Round up for batch count
    estimated_time_minutes = estimated_batches / 60  # ~60 batches per minute
    
    print(f"Target Group: {target_group}")
    print(f"Message Range: {start_id} to {end_id}")
    print(f"Total Messages: {total_messages}")
    print(f"Estimated Time: ~{estimated_time_minutes:.1f} minutes ({estimated_batches} batches)")
    print()
    
    # Confirm before proceeding
    print("⚠️  WARNING: This will permanently delete messages!")
    print("This action cannot be undone.")
    print()
    confirm = input("Are you sure you want to proceed? Type 'DELETE' to confirm: ")
    
    if confirm != "DELETE":
        print("❌ Operation cancelled.")
        return False
    
    print()
    print("🚀 Starting cleanup...")
    
    # Create deleter instance
    deleter = MessageDeleter(
        target_group_id=target_group,
        start_id=start_id,
        end_id=end_id,
        dry_run=False  # Set to True for testing
    )
    
    # Run deletion
    success = await deleter.delete_messages_in_range()
    
    if success:
        print()
        print("🎉 Cleanup completed successfully!")
        print("Your group has been cleaned up.")
    else:
        print()
        print("❌ Cleanup failed. Check the logs above for details.")
    
    return success


async def dry_run_cleanup():
    """Run a dry-run cleanup to see what would be deleted."""
    print("🔍 Dry Run - Group Cleanup Preview")
    print("=" * 40)
    
    config = Config()
    
    if not config.validate():
        print("❌ Invalid configuration. Please check your .env file.")
        return False
    
    target_group = config.target_group_id
    start_id = 1
    end_id = 17870
    total_messages = end_id - start_id + 1
    estimated_batches = (total_messages + 99) // 100  # Round up for batch count
    estimated_time_minutes = estimated_batches / 60  # ~60 batches per minute
    
    print(f"Target Group: {target_group}")
    print(f"Message Range: {start_id} to {end_id}")
    print(f"Total Messages: {total_messages}")
    print(f"Estimated Time: ~{estimated_time_minutes:.1f} minutes ({estimated_batches} batches)")
    print()
    print("🔍 DRY RUN MODE - No messages will be actually deleted")
    print()
    
    # Create deleter instance in dry-run mode
    deleter = MessageDeleter(
        target_group_id=target_group,
        start_id=start_id,
        end_id=end_id,
        dry_run=True
    )
    
    # Run dry-run deletion
    success = await deleter.delete_messages_in_range()
    
    print()
    if success:
        print("✅ Dry run completed. Run the actual cleanup when ready.")
    else:
        print("❌ Dry run failed. Check configuration and permissions.")
    
    return success


def main():
    """Main function to handle script execution."""
    if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
        print("Running in dry-run mode...")
        success = asyncio.run(dry_run_cleanup())
    else:
        success = asyncio.run(cleanup_development_messages())
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)