#!/usr/bin/env python3
"""
jobs/sink.py — Потребитель Kafka для ЛР6:
Читает тему событий, разбирает сообщения Avro по схеме из Karapace Schema Registry
и сохраняет часовые Parquet-файлы в слой raw.topic озера (s3://lake/raw/topic/dt=YYYY-MM-DD/hour=HH/).
"""

import os
import sys
import io
import struct
import json
import argparse
from datetime import datetime, timezone
from collections import defaultdict

import duckdb
from confluent_kafka import Consumer, KafkaError, TopicPartition
from fastavro import schemaless_reader, parse_schema
import requests
import boto3
from botocore.config import Config

TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "events")
GROUP_ID = os.environ.get("KAFKA_GROUP", "sink")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8081")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "lake")

SCHEMA_CACHE = {}

def get_schema_by_id(schema_id: int):
    if schema_id in SCHEMA_CACHE:
        return SCHEMA_CACHE[schema_id]
    url = f"{REGISTRY_URL}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    schema_json = json.loads(resp.json()["schema"])
    parsed = parse_schema(schema_json)
    SCHEMA_CACHE[schema_id] = parsed
    return parsed

def decode_confluent_avro(payload: bytes):
    if len(payload) < 5:
        raise ValueError("Payload too short for Confluent Avro wire format")
    magic, schema_id = struct.unpack(">bI", payload[:5])
    if magic != 0:
        raise ValueError(f"Unexpected magic byte: {magic}")
    schema = get_schema_by_id(schema_id)
    bio = io.BytesIO(payload[5:])
    return schemaless_reader(bio, schema)

def write_batch_to_s3(records, s3_client):
    if not records:
        return 0
    # Группировка по (dt, hour)
    by_partition = defaultdict(list)
    for r in records:
        ts = r.get("event_time")
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000.0, timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        dt_str = dt.strftime("%Y-%m-%d")
        hour_str = f"{dt.hour:02d}"
        by_partition[(dt_str, hour_str)].append(r)

    con = duckdb.connect()
    con.execute("LOAD httpfs;")
    con.execute(f"""
    CREATE SECRET s3sec (
        TYPE s3,
        KEY_ID 'studentaccess',
        SECRET 'studentsecretkey',
        ENDPOINT '{S3_ENDPOINT.replace("http://", "").replace("https://", "")}',
        URL_STYLE 'path',
        USE_SSL false,
        REGION 'stand'
    );
    """)

    written_count = 0
    for (dt_str, hour_str), batch in by_partition.items():
        temp_json = f"/tmp/topic_{dt_str}_{hour_str}_{os.getpid()}.json"
        temp_parquet = f"/tmp/topic_{dt_str}_{hour_str}_{os.getpid()}.parquet"
        with open(temp_json, "w", encoding="utf-8") as f:
            for item in batch:
                f.write(json.dumps(item, default=str) + "\n")
        con.execute(f"COPY (SELECT * FROM read_json_auto('{temp_json}')) TO '{temp_parquet}' (FORMAT parquet, COMPRESSION zstd);")
        s3_key = f"raw/topic/dt={dt_str}/hour={hour_str}/part-0.parquet"
        with open(temp_parquet, "rb") as pf:
            s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=pf.read())
        if os.path.exists(temp_json):
            os.remove(temp_json)
        if os.path.exists(temp_parquet):
            os.remove(temp_parquet)
        written_count += len(batch)
    return written_count

def run_sink(max_records=0, timeout_sec=10):
    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_NAME])

    s3 = boto3.client(
        's3', endpoint_url=S3_ENDPOINT, region_name='stand',
        aws_access_key_id='studentaccess', aws_secret_access_key='studentsecretkey',
        config=Config(s3={'addressing_style': 'path'})
    )

    print(f"Sink consumer started. Subscribed to topic: {TOPIC_NAME}, group: {GROUP_ID}")
    records = []
    total_read = 0
    empty_polls = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                if records:
                    write_batch_to_s3(records, s3)
                    consumer.commit()
                    records = []
                if empty_polls > timeout_sec:
                    print("No more messages in topic. Exiting.")
                    break
                continue

            empty_polls = 0
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break

            try:
                rec = decode_confluent_avro(msg.value())
                records.append(rec)
                total_read += 1
                if max_records and total_read >= max_records:
                    break
            except Exception as e:
                print(f"Failed to decode message: {e}")

            if len(records) >= 1000:
                write_batch_to_s3(records, s3)
                consumer.commit()
                records = []

        if records:
            write_batch_to_s3(records, s3)
            consumer.commit()

    finally:
        consumer.close()
    print(f"Sink completed. Total records processed and written: {total_read}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=5)
    args = parser.parse_args()
    run_sink(max_records=args.max, timeout_sec=args.timeout)
