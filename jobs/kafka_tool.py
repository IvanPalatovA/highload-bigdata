#!/usr/bin/env python3
"""
jobs/kafka_tool.py — Инструмент публикации и повтора (replay) для ЛР6:
1. Регистрация схемы в Karapace Schema Registry
2. Проверка несовместимости схемы (409 / is_compatible: false)
3. Публикация событий (produce / replay) с Confluent Avro сериализацией
"""

import os
import sys
import io
import struct
import json
import time
import argparse
from datetime import datetime, timezone
import requests
from fastavro import schemaless_writer, parse_schema
from confluent_kafka import Producer

REGISTRY_URL = os.environ.get("REGISTRY_URL", "http://localhost:8081")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def register_schema(subject, schema_path):
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    schema_str = json.dumps(schema_dict)
    url = f"{REGISTRY_URL}/subjects/{subject}/versions"
    resp = requests.post(url, json={"schema": schema_str}, timeout=10)
    print(f"Register schema response [{resp.status_code}]: {resp.text}")
    resp.raise_for_status()
    schema_id = resp.json()["id"]
    print(f"Schema registered with ID: {schema_id}")
    return schema_id, schema_dict

def test_incompatible_schema(subject):
    incompat_schema = {
        "type": "record",
        "name": "Event",
        "namespace": "stand",
        "fields": [
            {"name": "event_key", "type": "long"}, # Смена string на long
            {"name": "event_time", "type": "string"},
            {"name": "new_required_field", "type": "string"} # Новое обязательное поле без дефолта
        ]
    }
    schema_str = json.dumps(incompat_schema)
    # 1. Проверка совместимости
    comp_url = f"{REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
    r_check = requests.post(comp_url, json={"schema": schema_str}, timeout=10)
    print(f"Compatibility check [{r_check.status_code}]: {r_check.text}")
    # 2. Попытка регистрации
    reg_url = f"{REGISTRY_URL}/subjects/{subject}/versions"
    r_reg = requests.post(reg_url, json={"schema": schema_str}, timeout=10)
    print(f"Attempt to register incompatible schema [{r_reg.status_code}]: {r_reg.text}")
    return r_check.json().get("is_compatible", True), r_reg.status_code

def encode_confluent_avro(record, schema_id, parsed_schema):
    bio = io.BytesIO()
    # Confluent wire format: 0x00 + 4 bytes schema ID + avro binary
    bio.write(struct.pack(">bI", 0, schema_id))
    schemaless_writer(bio, parsed_schema, record)
    return bio.getvalue()

def produce_events(topic, schema_path, sample_file, max_events=500, replay_speed=1.0):
    subject = f"{topic}-value"
    schema_id, schema_dict = register_schema(subject, schema_path)
    parsed = parse_schema(schema_dict)

    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS, 'client.id': 'producer-tool'}
    p = Producer(conf)

    import gzip
    open_fn = gzip.open if sample_file.endswith(".gz") else open
    count = 0
    with open_fn(sample_file, "rt", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            ev = json.loads(line)
            # Приведение типов под схему Avro
            ev_key = str(ev.get("event_key", ev.get("id", "")))
            ts_str = ev.get("event_time", "")
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_millis = int(dt.timestamp() * 1000)
            except Exception:
                ts_millis = int(time.time() * 1000)

            avro_rec = {
                "event_key": ev_key,
                "event_time": ts_millis,
                "received_at": ev.get("received_at", datetime.now(timezone.utc).isoformat()),
                "source": ev.get("source", "github"),
                "id": str(ev.get("id", ev_key)),
                "type": ev.get("type", "CreateEvent"),
                "public": bool(ev.get("public", True)),
                "actor_id": int(ev.get("actor_id", 0)),
                "actor_login": str(ev.get("actor_login", "unknown")),
                "repo_id": int(ev.get("repo_id", 0)),
                "repo_name": str(ev.get("repo_name", "unknown/repo")),
                "org_id": ev.get("org_id"),
                "org_login": ev.get("org_login"),
                "ref_type": ev.get("ref_type"),
                "ref": ev.get("ref"),
                "master_branch": ev.get("master_branch"),
                "description": ev.get("description"),
                "pusher_type": ev.get("pusher_type"),
                "actor_avatar_url": ev.get("actor_avatar_url")
            }

            payload = encode_confluent_avro(avro_rec, schema_id, parsed)
            key_bytes = ev_key.encode("utf-8")
            p.produce(topic, key=key_bytes, value=payload)
            count += 1
            if count % 100 == 0:
                p.flush()
                print(f"Produced {count} events...")
            if max_events and count >= max_events:
                break
            if replay_speed > 1.0:
                time.sleep(0.001)

    p.flush()
    print(f"Total produced: {count} events into topic: {topic}")
    return count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["register", "test_incompat", "produce", "replay"])
    parser.add_argument("--topic", default="events")
    parser.add_argument("--schema", default="schemas/event.avsc")
    parser.add_argument("--file", default="")
    parser.add_argument("--max", type=int, default=500)
    parser.add_argument("--speed", type=float, default=1.0)
    args = parser.parse_args()

    if args.action == "register":
        register_schema(f"{args.topic}-value", args.schema)
    elif args.action == "test_incompat":
        test_incompatible_schema(f"{args.topic}-value")
    elif args.action in ("produce", "replay"):
        produce_events(args.topic, args.schema, args.file, max_events=args.max, replay_speed=args.speed)
