-- jobs/create_table.sql
-- ЛР4: Создание таблицы Iceberg lake.clean.events с hidden partitioning по дню.
-- Запуск: docker compose run --rm duckdb -init init.sql < jobs/create_table.sql
--
-- Требования варианта: схема события (поля Avro v1) + скрытое партиционирование days(event_time).

-- Создаём namespace, если не существует
CREATE SCHEMA IF NOT EXISTS lake.clean;

-- Создаём таблицу Iceberg
CREATE TABLE IF NOT EXISTS lake.clean.events (
    event_key       VARCHAR,
    event_time      TIMESTAMPTZ,
    received_at     VARCHAR,
    source          VARCHAR,
    id              VARCHAR,
    type            VARCHAR,
    public          BOOLEAN,
    actor_id        BIGINT,
    actor_login     VARCHAR,
    repo_id         BIGINT,
    repo_name       VARCHAR,
    org_id          BIGINT,
    org_login       VARCHAR,
    ref_type        VARCHAR,
    ref             VARCHAR,
    master_branch   VARCHAR,
    description     VARCHAR,
    pusher_type     VARCHAR
) USING iceberg
PARTITIONED BY (days(event_time));

