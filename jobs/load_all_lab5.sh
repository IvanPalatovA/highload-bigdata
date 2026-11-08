#!/usr/bin/env bash
# jobs/load_all_lab5.sh
# Автоматическая загрузка всех накопленных суток в ClickHouse (stg -> core -> mart)
# Запуск: bash jobs/load_all_lab5.sh

set -euo pipefail

STAND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "${STAND_DIR}"

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

echo "=================================================================="
echo ">>> Запуск пакетной загрузки ЛР5 в ClickHouse (stg -> core -> mart)"
echo "=================================================================="

for DAY in "${DAYS[@]}"; do
  echo -e "\n--------------------------------------------------"
  echo ">>> [1/2] Загрузка суток ${DAY} в stg.events из S3 Parquet..."
  docker compose exec -T warehouse clickhouse-client -u student --password student \
    --param_day="${DAY}" < jobs/stg_load.sql

  echo ">>> [2/2] Трансформация суток ${DAY} (stg -> core -> mart)..."
  docker compose exec -T warehouse clickhouse-client -u student --password student \
    --param_day="${DAY}" < jobs/mart_load.sql
  echo "✔ Сутки ${DAY} успешно загружены во все слои!"
done

echo -e "\n=================================================================="
echo "✔ Все сутки успешно загружены в ClickHouse!"
echo "=================================================================="

echo ">>> Итоговое число строк в слоях ClickHouse:"
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT 'stg.events' AS tbl, count(*) AS total_rows FROM stg.events
UNION ALL
SELECT 'core.events', count(*) FROM core.events
UNION ALL
SELECT 'mart.fact_events', count(*) FROM mart.fact_events
UNION ALL
SELECT 'mart.agg_hourly_events', count(*) FROM mart.agg_hourly_events
UNION ALL
SELECT 'core.dim_repo_scd', count(*) FROM core.dim_repo_scd;
"

