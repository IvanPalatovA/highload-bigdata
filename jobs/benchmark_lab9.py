#!/usr/bin/env python3
"""
jobs/benchmark_lab9.py — Исследование данных 3 путями (ЛР9):
Вопрос к данным: «Как меняется активность авторов (соотношение CreateEvent к DeleteEvent/ReleaseEvent)
по дням недели на всей накопленной истории наблюдений?»

3 Пути выполнения запроса:
Путь 1: Файлы Parquet очищенного слоя в S3 (DuckDB read_parquet)
Путь 2: Таблицы Lakehouse (Iceberg REST Catalog + DuckDB Iceberg Extension)
Путь 3: Аналитическая витрина DWH ClickHouse (mart.fact_events)
"""

import time
import duckdb
import clickhouse_connect

QUESTION = "Соотношение типов событий CreateEvent vs DeleteEvent по дням недели за всю историю"

def run_duckdb_s3():
    con = duckdb.connect()
    con.execute("LOAD httpfs;")
    con.execute("""
    CREATE SECRET s3sec (
        TYPE s3, KEY_ID 'studentaccess', SECRET 'studentsecretkey',
        ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'stand'
    );
    """)
    times = []
    rows = []
    for _ in range(3):
        t0 = time.perf_counter()
        res = con.execute("""
        SELECT
            dayname(event_time) as day_of_week,
            count_if(type = 'CreateEvent') as create_cnt,
            count_if(type = 'DeleteEvent') as delete_cnt,
            round(count_if(type = 'CreateEvent')::DOUBLE / nullif(count_if(type = 'DeleteEvent'), 0), 2) as ratio
        FROM read_parquet('s3://lake/clean/events/*/*.parquet', hive_partitioning = true)
        GROUP BY 1
        ORDER BY 1;
        """).fetchall()
        t1 = time.perf_counter()
        times.append(t1 - t0)
        rows = res
    times.sort()
    return times[1], rows

def run_duckdb_iceberg():
    con = duckdb.connect()
    con.execute("LOAD iceberg; LOAD httpfs;")
    con.execute("""
    CREATE SECRET s3sec (
        TYPE s3, KEY_ID 'studentaccess', SECRET 'studentsecretkey',
        ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false, REGION 'stand'
    );
    CREATE SECRET ic (TYPE iceberg, TOKEN '', ENDPOINT 'http://localhost:8181/catalog');
    ATTACH 'lake' AS lake (TYPE iceberg, SECRET ic, ACCESS_DELEGATION_MODE 'none');
    """)
    times = []
    rows = []
    for _ in range(3):
        t0 = time.perf_counter()
        res = con.execute("""
        SELECT
            dayname(event_time) as day_of_week,
            count_if(type = 'CreateEvent') as create_cnt,
            count_if(type = 'DeleteEvent') as delete_cnt,
            round(count_if(type = 'CreateEvent')::DOUBLE / nullif(count_if(type = 'DeleteEvent'), 0), 2) as ratio
        FROM lake.clean.events
        GROUP BY 1
        ORDER BY 1;
        """).fetchall()
        t1 = time.perf_counter()
        times.append(t1 - t0)
        rows = res
    times.sort()
    return times[1], rows

def run_clickhouse():
    client = clickhouse_connect.get_client(host="localhost", port=8123, username="student", password="student", database="mart")
    times = []
    rows = []
    for _ in range(3):
        t0 = time.perf_counter()
        res = client.query("""
        SELECT
            toDayOfWeek(event_time) as dow,
            countIf(type_key = 'CreateEvent') as create_cnt,
            countIf(type_key = 'DeleteEvent') as delete_cnt,
            round(create_cnt / nullIf(delete_cnt, 0), 2) as ratio
        FROM mart.fact_events
        GROUP BY 1
        ORDER BY 1;
        """)
        t1 = time.perf_counter()
        times.append(t1 - t0)
        rows = res.result_rows
    times.sort()
    return times[1], rows

def main():
    print("=" * 75)
    print(" ЛР9: Исследование данных 3 архитектурными путями (Вариант 93)")
    print(f" Вопрос: {QUESTION}")
    print("=" * 75)

    t_s3, res_s3 = run_duckdb_s3()
    print(f"1. S3 Parquet (DuckDB):      Медиана 3 запусков = {t_s3:.4f} с (Строк: {len(res_s3)})")

    t_ice, res_ice = run_duckdb_iceberg()
    print(f"2. Iceberg Lakehouse (DuckDB): Медиана 3 запусков = {t_ice:.4f} с (Строк: {len(res_ice)})")

    t_ch, res_ch = run_clickhouse()
    print(f"3. DWH ClickHouse (mart):    Медиана 3 запусков = {t_ch:.4f} с (Строк: {len(res_ch)})")

    print("-" * 75)
    print("Выводы по применимости путей:")
    print(" - S3 Parquet:      Идеально для Ad-hoc исследований и разового Data Science анализа.")
    print(" - Iceberg Lakehouse: Оптимально для регулярных пакетных отчётов, ELT-пайплайнов и Time Travel.")
    print(" - DWH ClickHouse:  Максимальная скорость отклика, эталон для интерактивных дашбордов BI (Metabase).")
    print("=" * 75)

if __name__ == "__main__":
    main()
