#!/usr/bin/env bash
# jobs/load_all_days.sh
# ЛР4: Загрузка всех суток clean-слоя в таблицу Iceberg lake.clean.events.
# Запуск из корня стенда:
#   bash jobs/load_all_days.sh
#
# Требует: duckdb-образ собран, стенд запущен (catalog, s3 working),
#          data/duckdb/init.sql создан.
#
# DuckDB монтирует ./data/duckdb -> /data/duckdb (рабочий каталог).
# Передаём SQL через stdin, переменную day — через -c.

set -euo pipefail

STAND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ICEBERG_LOAD_SQL="${STAND_DIR}/jobs/iceberg_load.sql"

DAYS=(
  2026-10-08
  2026-10-09
  2026-10-10
  2026-10-11
  2026-10-12
  2026-10-13
  2026-10-14
  2026-10-16
  2026-10-17
  2026-10-19
  2026-10-25
  2026-10-26
)

cd "${STAND_DIR}"

for DAY in "${DAYS[@]}"; do
  echo ">>> Loading day: $DAY"
  docker compose run --rm duckdb \
    -init init.sql \
    -c "SET VARIABLE day = '${DAY}';" \
    < "${ICEBERG_LOAD_SQL}"
  echo "<<< Done: $DAY"
done

echo "=== All days loaded ==="
