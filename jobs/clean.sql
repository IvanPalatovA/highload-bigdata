-- jobs/clean.sql
-- ЛР3: Копирование слоя очищенных данных (clean.events) в озеро данных (S3).
--
-- Читает локальный Parquet-слой (data/duckdb/parquet/events/), полученный ЛР2,
-- и записывает его в объектное хранилище S3/SeaweedFS под путь
-- s3://lake/clean/events/, с партиционированием по event_date.
--
-- Параметры записи выбраны по результатам бенчмарка ЛР2:
--   кодек zstd — максимальное сжатие при приемлемой скорости чтения;
--   ROW_GROUP_SIZE 122880 — один компактный блок на файл партиции.
--
-- Запуск:
--   docker compose run --rm duckdb < /opt/jobs/clean.sql
--
-- Переменные S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_REGION
-- берутся из .env, который передаётся контейнеру duckdb через env_file.

-- Подключаем расширение для работы с S3-совместимым хранилищем.
INSTALL httpfs;
LOAD httpfs;

-- Настройка подключения к S3/SeaweedFS через Secret Manager.
-- split_part убирает схему http:// из S3_ENDPOINT, оставляя host:port.
CREATE OR REPLACE SECRET s3_stand (
    TYPE       s3,
    PROVIDER   config,
    KEY_ID     getenv('S3_ACCESS_KEY'),
    SECRET     getenv('S3_SECRET_KEY'),
    REGION     getenv('S3_REGION'),
    ENDPOINT   split_part(getenv('S3_ENDPOINT'), '://', 2),
    URL_STYLE  'path',
    USE_SSL    false
);

-- Копируем все партиции слоя очищенных данных в S3.
-- Источник: /data/duckdb/parquet/events/ (результат to_parquet.sql из ЛР2).
-- Назначение: s3://lake/clean/events/ (партиционирование по event_date сохраняется).
-- union_by_name=true объединяет v1 и v2 партиции (разные наборы столбцов).
COPY (
    SELECT
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
        pusher_type,
        cast(null AS VARCHAR) AS actor_avatar_url,
        event_date
    FROM read_parquet(
        '/data/duckdb/parquet/events/**/*.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
    ORDER BY event_date, event_time
)
TO 's3://lake/clean/events'
(
    FORMAT          parquet,
    PARTITION_BY    (event_date),
    COMPRESSION     zstd,
    ROW_GROUP_SIZE  122880,
    OVERWRITE_OR_IGNORE true
);
