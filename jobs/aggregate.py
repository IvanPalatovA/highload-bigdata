#!/usr/bin/env python3
"""
jobs/aggregate.py — Пакетное задание PySpark для ЛР7:
Считает часовые агрегаты по таблице lake.clean.events за указанные сутки
и записывает результат в lake.clean.events_hourly с заменой партиции (replaceWhere).
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Target date in YYYY-MM-DD")
    args = parser.parse_args()
    target_date = args.date

    spark = SparkSession.builder \
        .appName(f"AggregateHourlyEvents-{target_date}") \
        .getOrCreate()

    print(f"Starting hourly aggregation for date: {target_date}")

    # Чтение таблицы lake.clean.events с предикатом по суткам
    df = spark.table("lake.clean.events").filter(
        F.to_date(F.col("event_time")) == F.lit(target_date)
    )

    # Расчет агрегатов: час, тип события, число событий, уникальные пользователи, уникальные репозитории
    agg_df = df.groupBy(
        F.date_trunc("hour", F.col("event_time")).alias("hour_slot"),
        F.to_date(F.col("event_time")).alias("event_date"),
        F.col("type")
    ).agg(
        F.count("*").alias("events_count"),
        F.countDistinct("actor_login").alias("unique_actors"),
        F.countDistinct("repo_id").alias("unique_repos"),
        F.sum(F.when(F.col("public"), 1).otherwise(0)).alias("public_events")
    )

    # Создание таблицы при необходимости
    spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.clean.events_hourly (
        hour_slot TIMESTAMP,
        event_date DATE,
        type STRING,
        events_count BIGINT,
        unique_actors BIGINT,
        unique_repos BIGINT,
        public_events BIGINT
    )
    USING iceberg
    PARTITIONED BY (event_date);
    """)

    # Идемпотентная запись с заменой только целевых суток (replaceWhere)
    print(f"Writing aggregates with replaceWhere: event_date = '{target_date}'")
    agg_df.writeTo("lake.clean.events_hourly") \
        .overwrite(F.col("event_date") == F.lit(target_date))

    print(f"Hourly aggregation for {target_date} completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()
