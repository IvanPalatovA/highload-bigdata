-- jobs/iceberg_load.sql
-- ЛР4: Загрузка слоя очищенных данных в таблицу Iceberg lake.clean.events.
-- Читает Parquet-файлы из S3 (clean/events/) и вставляет в Iceberg по одним суткам.
--
-- Запуск (пример для 2026-10-08):
--   docker compose run --rm duckdb -init init.sql \
--     -c "SET VARIABLE day = '2026-10-08';" \
--     < jobs/iceberg_load.sql
--
-- Внимание: DuckDB монтирует только data/duckdb и data/raw.
-- jobs/ передаётся через stdin (< jobs/iceberg_load.sql).

-- Параметр: дата загружаемых суток (формат YYYY-MM-DD)
-- Переопределяется через -c "SET VARIABLE day = '...'" перед запуском
SET VARIABLE day = '2026-10-08';

-- Удаляем существующие записи за эти сутки (идемпотентность)
DELETE FROM lake.clean.events
WHERE event_time >= getvariable('day')::TIMESTAMPTZ
  AND event_time <  (getvariable('day')::DATE + INTERVAL '1 day')::TIMESTAMPTZ;

-- Загружаем данные из S3 за указанные сутки
INSERT INTO lake.clean.events
SELECT
    event_key,
    event_time::TIMESTAMPTZ,
    received_at,
    source,
    id,
    type,
    public,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    org_id,
    org_login,
    ref_type,
    ref,
    master_branch,
    description,
    pusher_type
FROM read_parquet(
    's3://lake/clean/events/event_date=' || getvariable('day') || '/*.parquet',
    hive_partitioning = false
);
