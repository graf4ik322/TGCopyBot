#!/usr/bin/env python3
"""
Принудительная очистка всех файлов сессии и данных.
Используется когда приложение зависает из-за поврежденных сессий.
"""

import os
import subprocess
import sys

def run_command(cmd):
    """Выполнить команду и показать результат."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout:
            print(f"✅ {result.stdout.strip()}")
        if result.stderr and result.returncode != 0:
            print(f"⚠️ {result.stderr.strip()}")
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Ошибка выполнения команды '{cmd}': {e}")
        return False

def main():
    print("🧹 Принудительная очистка всех данных сессии...")
    print()
    
    # 1. Останавливаем контейнер
    print("1️⃣ Останавливаем Docker контейнер...")
    run_command("docker compose down")
    
    # 2. Удаляем Docker volume (ГЛАВНОЕ!)
    print("\n2️⃣ Удаляем Docker volume с данными...")
    if not run_command("docker volume rm tgcopybot_telegram_data"):
        print("⚠️ Пробуем принудительное удаление...")
        run_command("docker volume rm tgcopybot_telegram_data --force")
    
    # Дополнительная проверка - очищаем все неиспользуемые volumes
    print("🧹 Очищаем все неиспользуемые volumes...")
    run_command("docker volume prune -f")
    
    # 3. Удаляем все возможные файлы сессии
    print("\n3️⃣ Удаляем все файлы сессии...")
    files_to_remove = [
        "telegram_copier.session",
        "telegram_copier.session-journal", 
        "./data/telegram_copier.session",
        "./data/telegram_copier.session-journal",
        "last_message_id.txt",
        "./data/last_message_id.txt",
        "telegram_copier.lock",
        "./data/telegram_copier.lock", 
        "telegram_copier.log",
        "./data/telegram_copier.log"
    ]
    
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"✅ Удален: {file_path}")
            except Exception as e:
                print(f"⚠️ Не удалось удалить {file_path}: {e}")
        else:
            print(f"ℹ️ Не найден: {file_path}")
    
    # 4. Создаем чистую директорию данных
    print("\n4️⃣ Создаем чистую директорию данных...")
    try:
        os.makedirs("./data", exist_ok=True)
        os.chmod("./data", 0o755)
        print("✅ Директория ./data создана")
    except Exception as e:
        print(f"⚠️ Ошибка создания директории: {e}")
    
    print("\n🎉 Очистка завершена!")
    print("\n🚀 Теперь можно запустить заново:")
    print("   ./run_with_auth.sh")
    print("\n💡 При первом запуске потребуется новая авторизация")

if __name__ == "__main__":
    main()