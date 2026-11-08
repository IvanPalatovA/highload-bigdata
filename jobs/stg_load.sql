-- jobs/stg_load.sql
-- ЛР5: Загрузка слоя приёма stg.events из S3 (Parquet) за сутки __DAY__
-- Поддерживает прямую подстановку через sed

CREATE DATABASE IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.events
(
    event_key String,
    event_time DateTime64(3, 'UTC'),
    received_at String,
    source LowCardinality(String),
    id String,
    type LowCardinality(String),
    public UInt8,
    actor_id Int64,
    actor_login String,
    repo_id Int64,
    repo_name String,
    org_id Nullable(Int64),
    org_login Nullable(String),
    ref_type Nullable(String),
    ref Nullable(String),
    master_branch Nullable(String),
    description Nullable(String),
    pusher_type Nullable(String),
    actor_avatar_url Nullable(String),
    event_date Date
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (type, repo_id, event_time, id);

-- Загрузка очищенных Parquet-файлов из S3 за указанные сутки
INSERT INTO stg.events
SELECT
    event_key,
    event_time,
    received_at,
    source,
    id,
    type,
    toUInt8(public) AS public,
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
    cast(null, 'Nullable(String)') AS actor_avatar_url,
    toDate('__DAY__') AS event_date
FROM s3(
    'http://s3:9000/lake/clean/events/event_date=__DAY__/*.parquet',
    'studentaccess',
    'studentsecretkey',
    'Parquet'
);
