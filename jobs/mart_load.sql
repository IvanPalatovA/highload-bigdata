-- jobs/mart_load.sql
-- ЛР5: Пайплайн трансформации данных stg -> core -> mart за сутки __DAY__

-- ============================================================================
-- 1. Слой CORE (Нормализованное ядро)
-- ============================================================================
CREATE DATABASE IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.events
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

INSERT INTO core.events
SELECT * FROM stg.events
WHERE event_date = '__DAY__';


-- ============================================================================
-- 2. Слой MART (Аналитическая витрина "звезда" и материализованные представления)
-- ============================================================================
CREATE DATABASE IF NOT EXISTS mart;

-- 2.1 Измерение типов событий (Справочник)
CREATE TABLE IF NOT EXISTS mart.dim_type
(
    type_key LowCardinality(String),
    description String,
    is_release UInt8
)
ENGINE = MergeTree
ORDER BY type_key;

INSERT INTO mart.dim_type (type_key, description, is_release)
SELECT 'CreateEvent', 'Создание репозитория, ветки или тега', 0
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_type WHERE type_key = 'CreateEvent');

INSERT INTO mart.dim_type (type_key, description, is_release)
SELECT 'DeleteEvent', 'Удаление ветки или тега', 0
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_type WHERE type_key = 'DeleteEvent');

INSERT INTO mart.dim_type (type_key, description, is_release)
SELECT 'ReleaseEvent', 'Публикация релиза проекта', 1
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_type WHERE type_key = 'ReleaseEvent');

-- 2.2 Измерение пользователей (Actors)
CREATE TABLE IF NOT EXISTS mart.dim_actor
(
    actor_id Int64,
    actor_login String,
    actor_avatar_url Nullable(String),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY actor_id;

INSERT INTO mart.dim_actor
SELECT
    actor_id,
    argMax(actor_login, event_time) AS actor_login,
    argMax(actor_avatar_url, event_time) AS actor_avatar_url,
    max(event_time) AS updated_at
FROM core.events
WHERE event_date = '__DAY__'
GROUP BY actor_id;

-- 2.3 Агрегированная таблица для предрасчёта почасовых метрик
CREATE TABLE IF NOT EXISTS mart.agg_hourly_events
(
    hour DateTime,
    event_date Date,
    type_key LowCardinality(String),
    events_count UInt64
)
ENGINE = SummingMergeTree(events_count)
PARTITION BY event_date
ORDER BY (hour, type_key);

-- 2.4 Таблица фактов (Grain: 1 событие GitHub)
CREATE TABLE IF NOT EXISTS mart.fact_events
(
    event_id String,
    event_key String,
    event_time DateTime64(3, 'UTC'),
    event_date Date,
    type_key LowCardinality(String),
    repo_id Int64,
    actor_id Int64,
    public UInt8,
    events_count UInt32 DEFAULT 1
)
ENGINE = MergeTree
PARTITION BY event_date
ORDER BY (type_key, repo_id, event_time, event_id);

-- 2.5 Материализованное представление для наполнения агрегата
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_hourly_events
TO mart.agg_hourly_events
AS SELECT
    toStartOfHour(event_time) AS hour,
    event_date,
    type_key,
    count() AS events_count
FROM mart.fact_events
GROUP BY hour, event_date, type_key;

INSERT INTO mart.fact_events
SELECT
    id AS event_id,
    event_key,
    event_time,
    event_date,
    type AS type_key,
    repo_id,
    actor_id,
    public,
    1 AS events_count
FROM core.events
WHERE event_date = '__DAY__';

-- ============================================================================
-- 3. Измерение репозиториев с версионированием SCD Type 2
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.dim_repo_scd
(
    repo_id Int64,
    repo_name String,
    activity_class LowCardinality(String),
    daily_events UInt32,
    valid_from Date,
    valid_to Date,
    is_current UInt8
)
ENGINE = ReplacingMergeTree(daily_events)
ORDER BY (repo_id, valid_from);

CREATE TABLE IF NOT EXISTS mart.dim_repo
(
    repo_id Int64,
    repo_name String,
    activity_class LowCardinality(String),
    daily_events UInt32,
    valid_from Date,
    valid_to Date,
    is_current UInt8
)
ENGINE = ReplacingMergeTree(daily_events)
ORDER BY (repo_id, valid_from);

-- Расчёт SCD2 интервалов версионирования без использования мутаций с подзапросами
TRUNCATE TABLE IF EXISTS core.dim_repo_scd;

INSERT INTO core.dim_repo_scd (repo_id, repo_name, activity_class, daily_events, valid_from, valid_to, is_current)
WITH daily_stats AS (
    SELECT
        repo_id,
        argMax(repo_name, event_time) AS repo_name,
        multiIf(count() < 5, 'low', count() <= 20, 'medium', 'high') AS activity_class,
        toUInt32(count()) AS daily_events,
        event_date AS valid_from,
        leadInFrame(event_date, 1, toDate('9999-12-31')) OVER (
            PARTITION BY repo_id ORDER BY event_date ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
        ) AS next_date
    FROM core.events
    GROUP BY repo_id, event_date
)
SELECT
    repo_id,
    repo_name,
    activity_class,
    daily_events,
    valid_from,
    if(next_date = toDate('9999-12-31'), toDate('9999-12-31'), subtractDays(next_date, 1)) AS valid_to,
    if(next_date = toDate('9999-12-31'), 1, 0) AS is_current
FROM daily_stats;

TRUNCATE TABLE IF EXISTS mart.dim_repo;
INSERT INTO mart.dim_repo SELECT * FROM core.dim_repo_scd;
