-- jobs/iceberg_load.sql
-- ЛР4: Загрузка слоя очищенных данных в таблицу Iceberg lake.clean.events.
-- Читает Parquet-файлы из S3 (clean/events/) и вставляет в Iceberg по одним суткам.
-- Переменная day передаётся через пайп перед запуском:
--   (echo "SET VARIABLE day = 'YYYY-MM-DD';"; cat jobs/iceberg_load.sql) | docker compose run --rm -T duckdb -init init.sql

-- Удаляем существующие записи за эти сутки (идемпотентность)
DELETE FROM lake.clean.events
WHERE event_time >= getvariable('day')::TIMESTAMPTZ
  AND event_time <  (getvariable('day')::DATE + INTERVAL '1 day')::TIMESTAMPTZ;

-- Загружаем данные из S3 за указанные сутки с явным перечислением колонок
INSERT INTO lake.clean.events (
    event_key,
    event_time,
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
)
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
