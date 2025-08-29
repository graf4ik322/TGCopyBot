#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для извлечения строк с ошибками из output.log
Включает 2 строки контекста сверху и снизу для каждой ошибки
"""

import re
from collections import defaultdict

def extract_errors_with_context(log_file_path, output_file_path, context_lines=2):
    """
    Извлекает строки с ошибками и контекст вокруг них
    
    Args:
        log_file_path (str): Путь к файлу лога
        output_file_path (str): Путь для сохранения результата
        context_lines (int): Количество строк контекста сверху и снизу
    """
    
    print(f"Читаю файл: {log_file_path}")
    
    # Читаем весь файл в память для эффективного поиска
    with open(log_file_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    
    print(f"Всего строк в файле: {len(lines)}")
    
    # Ищем строки с ошибками
    error_lines = []
    for i, line in enumerate(lines):
        if 'Ошибка' in line:
            error_lines.append(i)
    
    print(f"Найдено строк с ошибками: {len(error_lines)}")
    
    # Собираем уникальные ошибки с контекстом
    unique_errors = defaultdict(list)
    
    for error_line_num in error_lines:
        # Получаем контекст
        start_line = max(0, error_line_num - context_lines)
        end_line = min(len(lines), error_line_num + context_lines + 1)
        
        # Создаем ключ для группировки (первая строка ошибки)
        error_key = lines[error_line_num].strip()
        
        # Собираем контекст
        context = []
        for i in range(start_line, end_line):
            line_num = i + 1  # Нумерация строк с 1
            prefix = ">>> " if i == error_line_num else "    "
            context.append(f"{prefix}{line_num:6d}: {lines[i].rstrip()}")
        
        # Добавляем разделитель между ошибками
        if unique_errors[error_key]:
            context.append("")
        
        unique_errors[error_key].extend(context)
    
    print(f"Найдено уникальных ошибок: {len(unique_errors)}")
    
    # Записываем результат в файл
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(f"ИЗВЛЕЧЕННЫЕ ОШИБКИ ИЗ {log_file_path}\n")
        f.write(f"Всего уникальных ошибок: {len(unique_errors)}\n")
        f.write(f"Контекст: {context_lines} строк сверху и снизу\n")
        f.write("=" * 80 + "\n\n")
        
        for i, (error_key, context_lines_list) in enumerate(unique_errors.items(), 1):
            f.write(f"ОШИБКА #{i}:\n")
            f.write("-" * 40 + "\n")
            f.write("\n".join(context_lines_list))
            f.write("\n\n")
    
    print(f"Результат сохранен в: {output_file_path}")
    
    # Выводим краткую статистику
    print("\nСТАТИСТИКА:")
    print(f"- Всего строк в логе: {len(lines):,}")
    print(f"- Строк с ошибками: {len(error_lines):,}")
    print(f"- Уникальных ошибок: {len(unique_errors):,}")
    print(f"- Контекст: ±{context_lines} строк")

if __name__ == "__main__":
    log_file = "output.log"
    output_file = "extracted_errors.txt"
    
    try:
        extract_errors_with_context(log_file, output_file)
    except FileNotFoundError:
        print(f"Ошибка: Файл {log_file} не найден!")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
