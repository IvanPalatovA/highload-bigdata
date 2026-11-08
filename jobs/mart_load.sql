-- jobs/mart_load.sql
-- ЛР5: Пайплайн трансформации данных stg -> core -> mart за сутки {day:Date}
-- Запуск:
--   docker compose exec -T warehouse clickhouse-client -u student --password student \
--     --param_day=YYYY-MM-DD < jobs/mart_load.sql

-- ============================================================================
-- 1. Слой CORE (Нормализованное ядро с историей SCD Type 2)
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

-- Идемпотентная перезагрузка суток в ядро
ALTER TABLE core.events DROP PARTITION IF EXISTS {day:Date};

INSERT INTO core.events
SELECT * FROM stg.events
WHERE event_date = {day:Date};

-- Измерение репозиториев с версионированием SCD Type 2
-- Отслеживает смену имени и класс активности (low < 5, medium 5-20, high > 20)
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

-- Расчёт суточного среза для SCD2
INSERT INTO core.dim_repo_scd
SELECT
    repo_id,
    argMax(repo_name, event_time) AS repo_name,
    multiIf(count() < 5, 'low', count() <= 20, 'medium', 'high') AS activity_class,
    toUInt32(count()) AS daily_events,
    {day:Date} AS valid_from,
    toDate('9999-12-31') AS valid_to,
    1 AS is_current
FROM core.events
WHERE event_date = {day:Date}
GROUP BY repo_id;

-- Закрытие предыдущей версии для репозиториев, у которых изменился класс или имя
ALTER TABLE core.dim_repo_scd
UPDATE
    valid_to = subtractDays({day:Date}, 1),
    is_current = 0
WHERE valid_from < {day:Date}
  AND valid_to = toDate('9999-12-31')
  AND repo_id IN (SELECT DISTINCT repo_id FROM core.events WHERE event_date = {day:Date});


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
WHERE event_date = {day:Date}
GROUP BY actor_id;

-- 2.3 Измерение репозиториев (копия SCD2 витрины)
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

INSERT INTO mart.dim_repo
SELECT * FROM core.dim_repo_scd
WHERE valid_from = {day:Date};

ALTER TABLE mart.dim_repo
UPDATE
    valid_to = subtractDays({day:Date}, 1),
    is_current = 0
WHERE valid_from < {day:Date}
  AND valid_to = toDate('9999-12-31')
  AND repo_id IN (SELECT DISTINCT repo_id FROM core.events WHERE event_date = {day:Date});

-- 2.4 Агрегированная таблица для предрасчёта почасовых метрик
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

-- 2.5 Таблица фактов (Grain: 1 событие GitHub)
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

-- 2.6 Материализованное представление для наполнения агрегата
CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_hourly_events
TO mart.agg_hourly_events
AS SELECT
    toStartOfHour(event_time) AS hour,
    event_date,
    type AS type_key,
    count() AS events_count
FROM mart.fact_events
GROUP BY hour, event_date, type_key;

-- Идемпотентная перезагрузка фактов и агрегатов за указанные сутки
ALTER TABLE mart.agg_hourly_events DROP PARTITION IF EXISTS {day:Date};
ALTER TABLE mart.fact_events DROP PARTITION IF EXISTS {day:Date};

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
WHERE event_date = {day:Date};
