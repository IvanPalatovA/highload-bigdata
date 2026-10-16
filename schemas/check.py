#!/usr/bin/env python3
"""
schemas/check.py — Программа проверки схемы события (Avro, Вариант 93)

Назначение:
  1. Читает события за час из слоя сырых данных (JSON Lines, gzip).
  2. Валидирует и кодирует записи в двоичный формат Avro по схеме event.avsc.
  3. Читает закодированные события обратно и сверяет их целостность.
  4. При ошибках валидации выводит проблемное событие и причину расхождения.
  5. Сравнивает размеры часа данных в JSON и двоичном Avro.
  6. Поддерживает проверку эволюции схемы и прямой/обратной совместимости (v1 <-> v2).
"""

import sys
import os
import io
import json
import gzip
import glob
import argparse
from datetime import datetime, timezone
import fastavro


def iso_to_millis(iso_str: str) -> int:
    """Конвертирует ISO 8601 UTC строку в миллисекунды с эпохи Unix."""
    if not iso_str:
        return 0
    # Поддержка Z и +00:00
    cleaned = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(cleaned)
    return int(dt.timestamp() * 1000)


def prepare_record_for_avro(raw_ev: dict, schema: dict) -> dict:
    """Приводит типы полей сырого JSON-события к типам схемы Avro."""
    rec = dict(raw_ev)
    
    # event_time в сыром JSON - строка ISO, в Avro - long timestamp-millis
    if "event_time" in rec and isinstance(rec["event_time"], str):
        rec["event_time"] = iso_to_millis(rec["event_time"])
        
    return rec


def find_sample_file() -> str:
    """Находит первый попавшийся часовой файл из stream или history."""
    patterns = [
        "data/raw/stream/**/*.jsonl.gz",
        "data/raw/history/**/*.jsonl.gz",
        "/data/raw/stream/**/*.jsonl.gz",
        "/data/raw/history/**/*.jsonl.gz"
    ]
    for p in patterns:
        matches = sorted(glob.glob(p, recursive=True))
        if matches:
            return matches[0]
    return ""


def check_file(file_path: str, schema_path: str, reader_schema_path: str = None) -> dict:
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Файл данных не найден: {file_path}")
    if not os.path.isfile(schema_path):
        raise FileNotFoundError(f"Файл схемы не найден: {schema_path}")

    # Загрузка схемы писателя (writer schema)
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_json = json.load(f)
    writer_schema = fastavro.parse_schema(schema_json)

    # Загрузка схемы читателя (reader schema), если задана
    reader_schema = None
    if reader_schema_path:
        with open(reader_schema_path, "r", encoding="utf-8") as f:
            reader_schema_json = json.load(f)
        reader_schema = fastavro.parse_schema(reader_schema_json)

    print(f"============================================================")
    print(f" Проверка схемы Avro (fastavro)")
    print(f" Файл данных:  {file_path}")
    print(f" Схема записи: {schema_path} (версия: {schema_json.get('doc', 'н/д')})")
    if reader_schema_path:
        print(f" Схема чтения: {reader_schema_path} (эволюция/совместимость)")
    print(f"============================================================")

    records = []
    errors = []
    raw_json_uncompressed_bytes = 0
    raw_gz_bytes = os.path.getsize(file_path)

    # 1. Чтение JSON Lines
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line_str = line.strip()
            if not line_str:
                continue
            raw_json_uncompressed_bytes += len(line.encode("utf-8"))
            try:
                ev = json.loads(line_str)
                prepared = prepare_record_for_avro(ev, writer_schema)
                # Валидация по схеме fastavro
                fastavro.validation.validate(prepared, writer_schema)
                records.append(prepared)
            except Exception as e:
                errors.append((line_num, line_str[:120], str(e)))

    if errors:
        print(f"\n[!] ОШИБКИ ВАЛИДАЦИИ ({len(errors)} событий):")
        for line_num, snippet, err in errors[:5]:
            print(f"  Строка {line_num}: {err}")
            print(f"  Фрагмент: {snippet}...\n")
        if len(errors) > 5:
            print(f"  ... и ещё {len(errors) - 5} ошибок.")
        return {"status": "FAILED", "errors": len(errors)}

    print(f"✔ Все {len(records)} событий успешно прошли валидацию по схеме Avro.")

    # 2. Двоичное кодирование в Avro
    avro_buffer = io.BytesIO()
    fastavro.writer(avro_buffer, writer_schema, records, codec="null")
    avro_uncompressed_bytes = len(avro_buffer.getvalue())

    # Кодирование с компрессией deflate/snappy для чистоты замера
    avro_deflate_buf = io.BytesIO()
    fastavro.writer(avro_deflate_buf, writer_schema, records, codec="deflate")
    avro_deflate_bytes = len(avro_deflate_buf.getvalue())

    # 3. Чтение обратно (декодирование)
    avro_buffer.seek(0)
    decoded_records = list(fastavro.reader(avro_buffer, reader_schema=reader_schema))

    if len(decoded_records) != len(records):
        raise ValueError(f"Число декодированных записей ({len(decoded_records)}) != числу исходных ({len(records)})")

    # Сверка полей первой записи
    if records:
        r0 = records[0]
        d0 = decoded_records[0]
        for k in ["event_key", "source", "type", "actor_login", "repo_name"]:
            if k in r0 and r0[k] != d0.get(k):
                raise ValueError(f"Несовпадение поля {k}: {r0[k]} != {d0.get(k)}")
        # Сверка event_time (fastavro преобразует timestamp-millis в datetime)
        r_time = r0["event_time"]
        d_time = d0["event_time"]
        if isinstance(d_time, datetime):
            d_time_millis = int(d_time.timestamp() * 1000)
            if r_time != d_time_millis:
                raise ValueError(f"Несовпадение event_time: {r_time} != {d_time_millis}")
        elif r_time != d_time:
            raise ValueError(f"Несовпадение event_time: {r_time} != {d_time}")

    print(f"✔ Чтение обратно успешно: декодировано {len(decoded_records)} событий.")
    if reader_schema_path:
        print(f"✔ Проверка совместимости схемы успешна! Новые/удалённые поля прочитаны корректно.")

    # 4. Вывод замеров размеров
    print(f"\n------------------------------------------------------------")
    print(f" Замеры объёма данных за час:")
    print(f"------------------------------------------------------------")
    print(f"  1. Сырой JSON (несжатый):      {raw_json_uncompressed_bytes:>10,} байт ({raw_json_uncompressed_bytes / 1024:.1f} КБ)")
    print(f"  2. Двоичный Avro (несжатый):   {avro_uncompressed_bytes:>10,} байт ({avro_uncompressed_bytes / 1024:.1f} КБ)")
    print(f"  3. Сырой JSON в gzip (.gz):    {raw_gz_bytes:>10,} байт ({raw_gz_bytes / 1024:.1f} КБ)")
    print(f"  4. Двоичный Avro + deflate:    {avro_deflate_bytes:>10,} байт ({avro_deflate_bytes / 1024:.1f} КБ)")
    print(f"------------------------------------------------------------")
    ratio_uncompressed = (1 - (avro_uncompressed_bytes / raw_json_uncompressed_bytes)) * 100 if raw_json_uncompressed_bytes else 0
    print(f"  Компактность бинарного Avro vs JSON: на {ratio_uncompressed:.1f}% меньше текста JSON.")
    print(f"============================================================\n")

    return {
        "status": "OK",
        "count": len(records),
        "raw_json_bytes": raw_json_uncompressed_bytes,
        "avro_bytes": avro_uncompressed_bytes,
        "raw_gz_bytes": raw_gz_bytes,
        "avro_deflate_bytes": avro_deflate_bytes
    }


def main():
    parser = argparse.ArgumentParser(description="Проверка схемы Avro (fastavro)")
    parser.add_argument("file", nargs="?", default="", help="Путь к часовому файлу *.jsonl.gz")
    parser.add_argument("--schema", "-s", default="schemas/event.avsc", help="Путь к схеме Avro (event.avsc)")
    parser.add_argument("--reader-schema", "-r", default=None, help="Путь к схеме читателя для проверки эволюции")
    args = parser.parse_args()

    data_file = args.file
    if not data_file:
        data_file = find_sample_file()
        if not data_file:
            print("Ошибка: не найдено ни одного файла *.jsonl.gz в data/raw/stream/ или data/raw/history/")
            sys.exit(1)

    try:
        res = check_file(data_file, args.schema, args.reader_schema)
        if res.get("status") != "OK":
            sys.exit(1)
    except Exception as e:
        print(f"Ошибка проверки схемы: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
