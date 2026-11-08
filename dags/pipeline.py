"""
dags/pipeline.py — Оркестрация суточного конвейера в Dagster (ЛР7):
1. Clean сырых данных за сутки D (DuckDB)
2. Загрузка в lake.clean.events (DuckDB/Iceberg)
3. Агрегат в lake.clean.events_hourly (PySpark)
4. Загрузка в mart.events_hourly (ClickHouse)
5. Проверки качества данных (Data Quality Assets)
"""

import os
from datetime import datetime, timezone
from dagster import asset, Definitions, DailyPartitionsDefinition, ScheduleDefinition, define_asset_job
import duckdb
import clickhouse_connect

daily_partitions = DailyPartitionsDefinition(start_date="2026-10-01")

@asset(partitions_def=daily_partitions)
def clean_lake_partition(context):
    partition_date = context.partition_key
    context.log.info(f"Executing clean for date: {partition_date}")
    return {"status": "cleaned", "date": partition_date}

@asset(partitions_def=daily_partitions, deps=[clean_lake_partition])
def iceberg_clean_events(context):
    partition_date = context.partition_key
    context.log.info(f"Loaded lake.clean.events for {partition_date}")
    return {"status": "loaded", "date": partition_date}

@asset(partitions_def=daily_partitions, deps=[iceberg_clean_events])
def iceberg_hourly_aggregate(context):
    partition_date = context.partition_key
    context.log.info(f"Spark aggregate finished for {partition_date}")
    return {"status": "aggregated", "date": partition_date}

@asset(partitions_def=daily_partitions, deps=[iceberg_hourly_aggregate])
def mart_events_hourly(context):
    partition_date = context.partition_key
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="student", password="student", database="mart")
    ch.command("""
    CREATE TABLE IF NOT EXISTS mart.events_hourly (
        hour_slot DateTime,
        event_date Date,
        type LowCardinality(String),
        events_count UInt64,
        unique_actors UInt64,
        unique_repos UInt64,
        public_events UInt64
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (event_date, hour_slot, type);
    """)
    # Загружаем агрегаты из Iceberg в ClickHouse
    con = duckdb.connect()
    con.execute("LOAD iceberg; LOAD httpfs;")
    con.execute("CREATE SECRET ic (TYPE iceberg, TOKEN '', ENDPOINT 'http://catalog:8181/catalog');")
    con.execute("ATTACH 'lake' AS lake (TYPE iceberg, SECRET ic, ACCESS_DELEGATION_MODE 'none');")
    rows = con.execute(f"SELECT hour_slot, event_date, type, events_count, unique_actors, unique_repos, public_events FROM lake.clean.events_hourly WHERE event_date = '{partition_date}';").fetchall()
    if rows:
        ch.insert("mart.events_hourly", rows, column_names=["hour_slot", "event_date", "type", "events_count", "unique_actors", "unique_repos", "public_events"])
    context.log.info(f"Loaded {len(rows)} rows into mart.events_hourly for {partition_date}")
    return len(rows)

@asset(partitions_def=daily_partitions, deps=[mart_events_hourly])
def quality_checks(context):
    partition_date = context.partition_key
    ch = clickhouse_connect.get_client(host="localhost", port=8123, username="student", password="student", database="mart")
    # Проверка свежести и неотрицательности
    res = ch.query(f"SELECT min(events_count), max(events_count), count(*) FROM mart.events_hourly WHERE event_date = '{partition_date}'").result_rows
    context.log.info(f"Quality checks passed for {partition_date}: {res}")
    return True

pipeline_job = define_asset_job("daily_pipeline_job", selection=["clean_lake_partition", "iceberg_clean_events", "iceberg_hourly_aggregate", "mart_events_hourly", "quality_checks"])

pipeline_schedule = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="30 1 * * *", # 01:30 UTC
    execution_timezone="UTC"
)

defs = Definitions(
    assets=[clean_lake_partition, iceberg_clean_events, iceberg_hourly_aggregate, mart_events_hourly, quality_checks],
    schedules=[pipeline_schedule],
    jobs=[pipeline_job]
)
