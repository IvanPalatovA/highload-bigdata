#!/usr/bin/env bash
# jobs/benchmark_3_paths.sh
# Комплексный бенчмарк трёх путей к данным и выполнение бонусного задания (10x масштабирование)
# Запуск: bash jobs/benchmark_3_paths.sh

set -euo pipefail

STAND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${STAND_DIR}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}   БЕНЧМАРК ТРЁХ ПУТЕЙ И БОНУСНОЕ ЗАДАНИЕ (ЛР5, ВАРИАНТ 93)      ${NC}"
echo -e "${BLUE}================================================================${NC}"

# 1. Замеры Путь 1: DuckDB над Parquet в S3
echo -e "\n${YELLOW}>>> [Путь 1] DuckDB over S3 Parquet (read_parquet)...${NC}"
P1_SQL="SELECT event_date, count(*) AS cnt FROM read_parquet('s3://lake/clean/events/*/*.parquet', hive_partitioning = true) WHERE type = 'CreateEvent' GROUP BY event_date ORDER BY event_date;"

for i in 1 2 3; do
  echo -n "  Запуск $i: "
  (echo ".timer on"; echo "${P1_SQL}") | docker compose run --rm -T duckdb -init init.sql 2>&1 | grep -i "Run Time" || true
done

# 2. Замеры Путь 2: DuckDB над Iceberg
echo -e "\n${YELLOW}>>> [Путь 2] DuckDB over Iceberg Lakehouse (lake.clean.events)...${NC}"
P2_SQL="SELECT CAST(event_time AS DATE) AS event_date, count(*) AS cnt FROM lake.clean.events WHERE type = 'CreateEvent' GROUP BY event_date ORDER BY event_date;"

for i in 1 2 3; do
  echo -n "  Запуск $i: "
  (echo ".timer on"; echo "${P2_SQL}") | docker compose run --rm -T duckdb -init init.sql 2>&1 | grep -i "Run Time" || true
done

# 3. Замеры Путь 3: ClickHouse Mart
echo -e "\n${YELLOW}>>> [Путь 3] ClickHouse Mart (mart.agg_hourly_events)...${NC}"
P3_SQL="SELECT event_date, sum(events_count) AS cnt FROM mart.agg_hourly_events WHERE type_key = 'CreateEvent' GROUP BY event_date ORDER BY event_date;"

for i in 1 2 3; do
  echo -n "  Запуск $i: "
  docker compose exec -T warehouse clickhouse-client -u student --password student --time -q "${P3_SQL}" 2>&1 | tail -n 1
done

echo -e "\n${BLUE}================================================================${NC}"
echo -e "${BLUE}   БОНУСНОЕ ЗАДАНИЕ: ЭКСПЕРИМЕНТ С 10X МАСШТАБИРОВАНИЕМ ДАННЫХ   ${NC}"
echo -e "${BLUE}================================================================${NC}"

echo -e "${CYAN}1. Создание тестового набора x10 для суток 2026-10-08 в ClickHouse...${NC}"
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
CREATE TABLE IF NOT EXISTS mart.fact_events_scale_test AS mart.fact_events;
TRUNCATE TABLE mart.fact_events_scale_test;

-- Вставка 10 копий суток 2026-10-08 с уникализированными ключами
INSERT INTO mart.fact_events_scale_test
SELECT
    concat(event_id, '_copy_', toString(n.number)) AS event_id,
    concat(event_key, '_copy_', toString(n.number)) AS event_key,
    event_time,
    event_date,
    type_key,
    repo_id,
    actor_id,
    public,
    1 AS events_count
FROM mart.fact_events
CROSS JOIN numbers(10) AS n
WHERE event_date = '2026-10-08';

SELECT 'Строк в x10 наборе:' AS metric, count(*) AS cnt FROM mart.fact_events_scale_test;
"

echo -e "\n${CYAN}2. Замер времени выполнения аналитического запроса на x10 наборе в ClickHouse...${NC}"
SCALE_SQL="SELECT event_date, count(*) FROM mart.fact_events_scale_test WHERE type_key = 'CreateEvent' GROUP BY event_date;"
docker compose exec -T warehouse clickhouse-client -u student --password student --time -q "${SCALE_SQL}"

echo -e "\n${CYAN}3. Безопасное удаление тестового набора после замера (чистота среды)...${NC}"
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
DROP TABLE IF EXISTS mart.fact_events_scale_test;
"
echo -e "${GREEN}✔ Тестовые таблицы удалены, рабочие наборы сохранены в исходном виде!${NC}"
echo -e "${GREEN}✔ Бенчмарк и бонусное задание успешно выполнены!${NC}"
