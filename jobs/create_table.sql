-- jobs/create_table.sql
-- ЛР4: Создание таблицы Iceberg lake.clean.events с hidden partitioning по дню.
-- В DuckDB каталог lake уже подключен как (TYPE iceberg), поэтому 'USING iceberg' писать не нужно.

CREATE SCHEMA IF NOT EXISTS lake.clean;

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
)
PARTITIONED BY (days(event_time));
