-- jobs/to_parquet_v2.sql
-- Запись новых суток по схеме v2 с новым полем actor_avatar_url.
-- Старые партиции остаются записанными схемой v1 (без этого поля).

COPY (
    SELECT
        CAST(event_key AS VARCHAR) AS event_key,
        CAST(event_time AS TIMESTAMP) AS event_time,
        CAST(received_at AS VARCHAR) AS received_at,
        CAST(source AS VARCHAR) AS source,
        CAST(id AS VARCHAR) AS id,
        CAST(type AS VARCHAR) AS type,
        CAST(public AS BOOLEAN) AS public,
        CAST(actor_id AS BIGINT) AS actor_id,
        CAST(actor_login AS VARCHAR) AS actor_login,
        CAST(repo_id AS BIGINT) AS repo_id,
        CAST(repo_name AS VARCHAR) AS repo_name,
        CAST(org_id AS BIGINT) AS org_id,
        CAST(org_login AS VARCHAR) AS org_login,
        CAST(ref_type AS VARCHAR) AS ref_type,
        CAST(ref AS VARCHAR) AS ref,
        CAST(master_branch AS VARCHAR) AS master_branch,
        CAST(description AS VARCHAR) AS description,
        CAST(pusher_type AS VARCHAR) AS pusher_type,
        'https://avatars.githubusercontent.com/u/' || CAST(actor_id AS VARCHAR) AS actor_avatar_url,
        CAST(event_time AS DATE) AS event_date
    FROM (
        SELECT DISTINCT ON (event_key) *
        FROM read_json_auto([
            '/data/raw/stream/dt=2026-10-16/**/*.jsonl.gz'
        ], hive_partitioning = false)
    )
    ORDER BY event_time
)
TO '/data/duckdb/parquet/events'
(
    FORMAT parquet,
    PARTITION_BY (event_date),
    OVERWRITE_OR_IGNORE true
);
