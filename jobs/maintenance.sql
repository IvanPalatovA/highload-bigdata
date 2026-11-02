-- jobs/maintenance.sql
-- ЛР4: Обслуживание таблицы Iceberg lake.clean.events.
-- Запросы для Spark SQL. Запуск:
--   docker compose run --rm spark spark-sql -f /opt/jobs/maintenance.sql
--   или интерактивно: docker compose run --rm spark spark-sql

-- ============================================================
-- 1. Просмотр снимков (snapshots)
-- ============================================================
SELECT snapshot_id, committed_at, operation
FROM lake.clean.events.snapshots
ORDER BY committed_at;

-- ============================================================
-- 2. Кол-во файлов и суммарный размер до компакции
-- ============================================================
SELECT count(*) AS file_count, sum(file_size_in_bytes) AS total_bytes
FROM lake.clean.events.files;

-- ============================================================
-- 3. Просмотр данных в прошлом снимке (time travel)
-- Замените <snapshot_id> на нужный snapshot_id из п.1
-- ============================================================
-- SELECT count(*) FROM lake.clean.events VERSION AS OF <snapshot_id>;

-- ============================================================
-- 4. Откат к прошлому снимку (rollback)
-- Замените <snapshot_id> на snapshot_id до ошибочной записи
-- ============================================================
-- CALL lake.system.rollback_to_snapshot('clean.events', <snapshot_id>);

-- ============================================================
-- 5. Компакция файлов (rewrite_data_files)
-- ============================================================
CALL lake.system.rewrite_data_files(table => 'clean.events');

-- ============================================================
-- 6. Кол-во файлов и размер ПОСЛЕ компакции
-- ============================================================
SELECT count(*) AS file_count, sum(file_size_in_bytes) AS total_bytes
FROM lake.clean.events.files;

-- ============================================================
-- 7. Истечение снимков (expire_snapshots) — оставить 1
-- Замените дату на завтрашнюю (после компакции)
-- ============================================================
CALL lake.system.expire_snapshots(
    table       => 'clean.events',
    older_than  => TIMESTAMP '2099-01-01 00:00:00',
    retain_last => 1
);

-- ============================================================
-- 8. Добавление колонки без перезаписи файлов (schema evolution)
-- ============================================================
ALTER TABLE lake.clean.events ADD COLUMN actor_avatar_url STRING;

-- ============================================================
-- 9. Проверка: старые строки имеют NULL в новой колонке
-- ============================================================
SELECT actor_avatar_url, count(*) AS cnt
FROM lake.clean.events
GROUP BY 1
ORDER BY 1 NULLS FIRST
LIMIT 5;

