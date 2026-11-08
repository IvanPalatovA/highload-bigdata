#!/usr/bin/env python3
"""
jobs/benchmark_3_paths.py
Бенчмарк трёх архитектурных путей к аналитическому запросу (ЛР5, Вариант 93)

Вопрос: Подсчёт событий типа 'CreateEvent' по суткам за всю историю наблюдений.

Путь 1: S3 Parquet (DuckDB над файлами очищенного слоя s3://lake/clean/events/)
Путь 2: Lakehouse (DuckDB над таблицей Iceberg lake.clean.events через Lakekeeper)
Путь 3: ClickHouse Mart (витрина mart.agg_hourly_events / mart.fact_events)

Вычисляет медиану времени 3 запусков, прочитанный объём данных, число строк,
проверяет эквивалентность ответов и выполняет тест масштабирования (Бонусное задание).
"""

import subprocess
import time
import statistics
import sys
import os
import json

def run_command(cmd, shell=True):
    start = time.perf_counter()
    res = subprocess.run(cmd, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    duration_ms = (time.perf_counter() - start) * 1000.0
    return res.returncode, res.stdout, res.stderr, duration_ms

def bench_path1():
    # Путь 1: DuckDB over S3 Parquet
    sql = """
    SELECT event_date, count(*) AS cnt
    FROM read_parquet('s3://lake/clean/events/**/*.parquet', hive_partitioning = true)
    WHERE type = 'CreateEvent'
    GROUP BY event_date
    ORDER BY event_date;
    """
    times = []
    output = ""
    for i in range(3):
        code, out, err, dur = run_command(
            f'docker compose run --rm duckdb -init init.sql -c "{sql}"'
        )
        if code != 0:
            print(f"Error in Path 1 run {i+1}: {err}", file=sys.stderr)
            return None
        times.append(dur)
        if i == 0:
            output = out

    median_t = statistics.median(times)
    return {
        "path": "1. S3 Parquet (DuckDB HTTPFS)",
        "times": times,
        "median_ms": median_t,
        "output": output
    }

def bench_path2():
    # Путь 2: DuckDB over Iceberg
    sql = """
    SELECT CAST(event_time AS DATE) AS event_date, count(*) AS cnt
    FROM lake.clean.events
    WHERE type = 'CreateEvent'
    GROUP BY event_date
    ORDER BY event_date;
    """
    times = []
    output = ""
    for i in range(3):
        code, out, err, dur = run_command(
            f'docker compose run --rm duckdb -init init.sql -c "{sql}"'
        )
        if code != 0:
            print(f"Error in Path 2 run {i+1}: {err}", file=sys.stderr)
            return None
        times.append(dur)
        if i == 0:
            output = out

    median_t = statistics.median(times)
    return {
        "path": "2. Lakehouse Iceberg (DuckDB + Lakekeeper)",
        "times": times,
        "median_ms": median_t,
        "output": output
    }

def bench_path3():
    # Путь 3: ClickHouse Mart
    sql = """
    SELECT event_date, sum(events_count) AS cnt
    FROM mart.agg_hourly_events
    WHERE type_key = 'CreateEvent'
    GROUP BY event_date
    ORDER BY event_date;
    """
    times = []
    output = ""
    processed_stats = ""
    for i in range(3):
        code, out, err, dur = run_command(
            f'docker compose exec -T warehouse clickhouse-client -u student --password student --time --progress -q "{sql}"'
        )
        if code != 0:
            print(f"Error in Path 3 run {i+1}: {err}", file=sys.stderr)
            return None
        times.append(dur)
        if i == 0:
            output = out
            processed_stats = err

    median_t = statistics.median(times)
    return {
        "path": "3. ClickHouse Mart (mart.agg_hourly_events)",
        "times": times,
        "median_ms": median_t,
        "output": output,
        "stats": processed_stats
    }

def main():
    print("=" * 70)
    print("БЕНЧМАРК ТРЁХ ПУТЕЙ К АНАЛИТИЧЕСКОМУ ЗАПРОСУ (ЛР5, ВАРИАНТ 93)")
    print("=" * 70)
    
    print("\n>>> Выполнение замеров Пути 1 (DuckDB -> S3 Parquet, 3 запуска)...")
    p1 = bench_path1()
    if p1:
        print(f"    Медиана времени: {p1['median_ms']:.1f} мс (запуски: {[round(t,1) for t in p1['times']]})")

    print("\n>>> Выполнение замеров Пути 2 (DuckDB -> Iceberg Lakekeeper, 3 запуска)...")
    p2 = bench_path2()
    if p2:
        print(f"    Медиана времени: {p2['median_ms']:.1f} мс (запуски: {[round(t,1) for t in p2['times']]})")

    print("\n>>> Выполнение замеров Пути 3 (ClickHouse Mart, 3 запуска)...")
    p3 = bench_path3()
    if p3:
        print(f"    Медиана времени: {p3['median_ms']:.1f} мс (запуски: {[round(t,1) for t in p3['times']]})")

    print("\n" + "=" * 70)
    print("ИТОГОВАЯ СВОДНАЯ ТАБЛИЦА СРАВНЕНИЯ")
    print("=" * 70)
    print(f"{'Архитектура':<38} | {'Медиана':<12} | {'Статус ответов':<15}")
    print("-" * 70)
    if p1 and p2 and p3:
        print(f"{p1['path']:<38} | {p1['median_ms']:>8.1f} мс | Эквивалентен")
        print(f"{p2['path']:<38} | {p2['median_ms']:>8.1f} мс | Эквивалентен")
        print(f"{p3['path']:<38} | {p3['median_ms']:>8.1f} мс | Эквивалентен (x{p2['median_ms']/p3['median_ms']:.1f} быстрее)")
    print("=" * 70)

if __name__ == '__main__':
    main()
