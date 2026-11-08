# 📋 Демонстрация ЛР4: Таблицы Apache Iceberg (Вариант 93) — code.md

> [!IMPORTANT]
> Все команды выполняются из каталога стенда:
> ```bash
> cd stand
> ```
> Вы можете выполнять команды по очереди вручную либо запустить автоматический скрипт проверки:
> ```bash
> bash demo.sh
> ```

---

## Шаг 0. Проверка работоспособности стенда

**Что говоришь преподавателю:**  
*«Профиль ЛР4 поднят. Работают сервисы объектного хранилища S3 (порт 9000), REST-каталога таблиц Lakekeeper (порт 8181), базы каталога Postgres и сборщика данных.»*

### Команда:
```bash
docker compose ps
```

### Ожидаемый вывод:
```text
NAME         IMAGE                                COMMAND                  SERVICE      STATUS
catalog      quay.io/lakekeeper/catalog:v0.13.6   "/home/nonroot/lakek…"   catalog      Up (healthy)
catalog-db   postgres:17.11-alpine                "docker-entrypoint.s…"   catalog-db   Up (healthy)
collector    bigdata-stand/collector:1            "/usr/local/bin/entr…"   collector    Up
s3           chrislusf/seaweedfs:4.46             "/entrypoint.sh serv…"   s3           Up (healthy)
```

---

## Шаг 1. Проверка параметров варианта в .env

**Что говоришь преподавателю:**  
*«Вариант 93: источник данных — GitHub Archive (GH Archive), фильтр — CreateEvent, DeleteEvent, ReleaseEvent, сериализация Avro, движок запросов — DuckDB.»*

### Команда:
```bash
grep -E '^(VARIANT|SOURCE|SERDE|S3|ENGINE|COMPOSE_FILE)=' .env
```

### Ожидаемый вывод:
```text
COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.engine-duckdb.yaml
VARIANT=93
SOURCE=github
SERDE=avro
S3=seaweedfs
ENGINE=duckdb
```

---

## Шаг 2. Конфигурация подключений к каталогу Iceberg

**Что говоришь преподавателю:**  
*«DuckDB подключается к S3 и REST-каталогу через секреты в init.sql и монтирует каталог как lake через команду ATTACH. Spark подключается через файл конфигурации spark-defaults.conf с использованием SparkCatalog и S3FileIO.»*

### Команда 2.1 (DuckDB init.sql):
```bash
cat data/duckdb/init.sql
```

### Ожидаемый вывод:
```sql
LOAD httpfs;
LOAD iceberg;

CREATE SECRET s3sec (
    TYPE s3,
    KEY_ID 'studentaccess',
    SECRET 'studentsecretkey',
    ENDPOINT 's3:9000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'stand'
);

CREATE SECRET ic (
    TYPE iceberg,
    TOKEN '',
    ENDPOINT 'http://catalog:8181/catalog'
);

ATTACH 'lake' AS lake (
    TYPE iceberg,
    SECRET ic,
    ACCESS_DELEGATION_MODE 'none'
);
```

### Команда 2.2 (Spark spark-defaults.conf):
```bash
cat conf/spark/spark-defaults.conf
```

### Ожидаемый вывод:
```properties
spark.sql.catalog.lake                          org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lake.type                     rest
spark.sql.catalog.lake.uri                      ${ENV:CATALOG_URI}
spark.sql.catalog.lake.warehouse                ${ENV:CATALOG_WAREHOUSE}
spark.sql.catalog.lake.io-impl                  org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.lake.s3.endpoint              ${ENV:S3_ENDPOINT}
spark.sql.catalog.lake.client.region            ${ENV:S3_REGION}
spark.sql.catalog.lake.s3.path-style-access     true
spark.sql.catalog.lake.s3.access-key-id        ${ENV:S3_ACCESS_KEY}
spark.sql.catalog.lake.s3.secret-access-key    ${ENV:S3_SECRET_KEY}
spark.sql.defaultCatalog                        lake
```

---

## Шаг 3. Схема таблицы и скрытое партиционирование (DDL)

**Что говоришь преподавателю:**  
*«Таблица создана со схемой из 18 полей Avro v1 и со скрытым партиционированием по дню event_time: PARTITIONED BY (days(event_time)). В отличие от Hive, нет искусственной колонки партиции — Iceberg сам вычисляет день из метки времени.»*

### Команда:
```bash
cat jobs/create_table.sql
```

### Ожидаемый вывод:
```sql
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
) USING iceberg
PARTITIONED BY (days(event_time));
```

---

## Шаг 4. Проверка наполнения таблицы (139 530 строк)

**Что говоришь преподавателю:**  
*«Загрузка выполнена скриптом jobs/load_all_days.sh. В таблицу залиты 7 дней исторического среза (120 760 строк) и потоковые порции, итого ровно 139 530 строк. Загрузка идемпотентна благодаря предварительному DELETE за целевые сутки.»*

### Команда:
```bash
docker compose run --rm duckdb -init init.sql -c "SELECT count(*) AS total_rows FROM lake.clean.events;"
```

### Ожидаемый вывод:
```text
┌──────────────┐
│  total_rows  │
│    int64     │
├──────────────┤
│       139530 │
└──────────────┘
```

---

## Шаг 5. Главная фишка: Отсечение файлов по предикату (Predicate Pushdown)

**Что говоришь преподавателю:**  
*«Сравним два аналитических запроса через EXPLAIN ANALYZE. Без фильтра DuckDB сканирует все 12 суточных файлов таблицы (308 KiB трафика). При добавлении WHERE по дате движок отсекает 11 файлов на этапе чтения манифестов Iceberg и читает ровно 1 файл (151 KiB трафика).»*

### Команда 5.1 — Запрос БЕЗ фильтра (сканирует ВСЕ 12 файлов):
```bash
docker compose run --rm duckdb -init init.sql -c "EXPLAIN ANALYZE SELECT type, count(*) FROM lake.clean.events GROUP BY 1;"
```

### Что показать в выводе:
* Блок `TABLE_SCAN (ICEBERG_SCAN)`:
  * **`Total Files Read: 12`**
  * `139,530 rows`
* Блок `HTTPFS HTTP Stats`:
  * **`in: 308.4 KiB`**
  * **`#GET: 37`**

---

### Команда 5.2 — Запрос С фильтром по дате (сканирует РОВНО 1 файл!):
```bash
docker compose run --rm duckdb -init init.sql -c "EXPLAIN ANALYZE SELECT type, count(*) FROM lake.clean.events WHERE event_time >= TIMESTAMPTZ '2026-10-10 00:00:00' AND event_time < TIMESTAMPTZ '2026-10-11 00:00:00' GROUP BY 1;"
```

### Что показать в выводе:
* Блок `TABLE_SCAN (ICEBERG_SCAN)`:
  * **`Total Files Read: 1`**  *(отсечено 11 файлов!)*
  * `16,904 rows`
* Блок `HTTPFS HTTP Stats`:
  * **`in: 151.8 KiB`**  *(трафик уменьшился более чем в 2 раза!)*
  * **`#GET: 16`**

---

## Шаг 6. Просмотр снимков таблицы (Snapshots) в Spark SQL

**Что говоришь преподавателю:**  
*«Каждая операция записи в Iceberg фиксирует снимок (Snapshot). Посмотрим список снимков через системную таблицу snapshots.»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT snapshot_id, committed_at, operation FROM lake.clean.events.snapshots;"
```

### Ожидаемый вывод:
```text
8914981769347043057	2026-11-02 11:37:32.373	replace
```

---

## Шаг 7. Путешествие во времени (Time Travel)

**Что говоришь преподавателю:**  
*«Благодаря снимкам мы можем прочитать прошлое состояние таблицы без восстановления бэкапов. Например, снимок после загрузки только истории возвращает 120 760 строк (без потока).»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT count(*) FROM lake.clean.events VERSION AS OF 3125082485831910116;"
```

### Ожидаемый вывод:
```text
120760
```

---

## Шаг 8. Ошибочная запись и мгновенный откат (Rollback)

**Что говоришь преподавателю:**  
*«Сымитируем сбой: вставим дубликаты. Число строк вырастет. Затем вызовем процедуру rollback_to_snapshot, и таблица мгновенно вернется к 139 530 строкам. Откат происходит за доли секунды, так как каталог просто меняет указатель текущего снимка в PostgreSQL без перезаписи файлов.»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "
-- 1. Ошибочная вставка
INSERT INTO lake.clean.events SELECT * FROM lake.clean.events WHERE event_time >= '2026-10-26 23:00:00';
SELECT count(*) AS count_after_error FROM lake.clean.events;

-- 2. Мгновенный откат к снимку 8914981769347043057
CALL lake.system.rollback_to_snapshot('clean.events', 8914981769347043057);
SELECT count(*) AS count_after_rollback FROM lake.clean.events;
"
```

### Ожидаемый вывод:
```text
count_after_error:     139700+ (число строк выросло)
count_after_rollback:  139530 (число строк мгновенно вернулось в норму!)
```

---

## Шаг 9. Компакция мелких файлов (Compaction)

**Что говоришь преподавателю:**  
*«При частой потоковой записи возникает проблема мелких файлов. За сутки 2026-10-26 накопилось 5 часовых файлов. Процедура rewrite_data_files склеила их в 1 оптимизированный Parquet-файл размером 436 КБ. Читатели в этот момент продолжали читать старый снимок без блокировок.»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "CALL lake.system.rewrite_data_files(table => 'clean.events');"
```

### Ожидаемый вывод:
```text
rewritten_data_files_count: 5, added_data_files_count: 1
```

---

## Шаг 10. Устаревание снимков (Expire Snapshots)

**Что говоришь преподавателю:**  
*«Старые файлы после компакции и отката занимают место в S3. Процедура expire_snapshots удаляет метаданные старых снимков и физически стирает осиротевшие файлы данных.»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "
CALL lake.system.expire_snapshots(table => 'clean.events', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1);
SELECT snapshot_id, committed_at, operation FROM lake.clean.events.snapshots;
"
```

### Ожидаемый вывод:
```text
8914981769347043057	2026-11-02 11:37:32.373	replace
Fetched 1 row(s) — остался 1 актуальный снимок, мусор очищен.
```

---

## Шаг 11. Эволюция схемы (Schema Evolution) без перезаписи файлов

**Что говоришь преподавателю:**  
*«Добавили новое поле actor_avatar_url через ALTER TABLE. В Iceberg поля адресуются целочисленными field-id. Поэтому сами 12 файлов Parquet в S3 не перезаписывались, а существующие строки при чтении возвращают NULL.»*

### Команда:
```bash
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT actor_avatar_url, count(*) FROM lake.clean.events GROUP BY 1;"
```

### Ожидаемый вывод:
```text
NULL	139530
Time taken: ~2-3 seconds, Fetched 1 row(s)
```

---

## Шаг 12. Дополнительное задание: Разрешение конфликтов параллельных коммитов (OCC)

**Что говоришь преподавателю:**  
*«В Iceberg используется оптимистичная блокировка (OCC). Если две сессии одновременно обновляют одни и те же сутки — срабатывает принцип first-commit-wins: первая сессия фиксирует коммит, а вторая получает CommitFailedException, защищая данные от потери обновления. Если же сессии пишут в разные непересекающиеся сутки, Iceberg выполняет автоматический rebase и успешно фиксирует оба коммита.»*

### Схема разрешения конфликта:
```
DuckDB (Сессия 1)  ──[Удаляет 2026-10-14]──> Коммит в Lakekeeper ──> 200 OK (Победил!)
Spark  (Сессия 2)  ──[Обновляет 2026-10-14]─> Коммит в Lakekeeper ──> 409 Conflict (CommitFailedException)
```

---

## Шаг 13. Проверка каталога CATALOG.md и журнала NOTES.md

**Что говоришь преподавателю:**  
*«В CATALOG.md оформлен паспорт таблицы lake.clean.events и составлена схема связей сервисов. В NOTES.md зафиксированы все протоколы замеров до и после для всех шагов ЛР4.»*

### Команда:
```bash
head -n 25 CATALOG.md
tail -n 40 NOTES.md
```

---

## 🎯 Шпаргалка ответов на частые вопросы преподавателя

| Вопрос | Ответ |
|---|---|
| **Где физически лежат метаданные таблицы?** | Метаданные (JSON и Avro) лежат в S3 в папке `metadata/`. А ссылка на актуальный корневой файл метаданных хранится в каталоге Lakekeeper в базе PostgreSQL. |
| **Почему скрытое партиционирование лучше Hive?** | В Hive партиция — это явная папка `dt=...`, и запрос обязан фильтровать именно по ней. В Iceberg партиционирование — это функция от поля (`days(event_time)`). Запрос `WHERE event_time >= ...` отсекает файлы автоматически. |
| **Почему Rollback выполняется мгновенно?** | Потому что данные не перемещаются и не перезаписываются; в каталоге просто меняется указатель `current-snapshot-id` на идентификатор старого снимка. |
| **Что делает rewrite_data_files? Блокирует ли он чтение?** | Он читает мелкие файлы и перезаписывает их в меньшее количество крупных. Чтение не блокируется: читатели продолжают видеть старый снимок до момента коммита. |
| **Зачем нужен expire_snapshots?** | После частых обновлений и компакции старые файлы данных продолжают занимать место в S3 для поддержки Time Travel. `expire_snapshots` удаляет метаданные старых снимков и физически стирает осиротевшие файлы. |
| **Почему при добавлении колонки не нужно переписывать файлы?** | Iceberg адресует колонки по `field-id`, а не по именам. При добавлении колонки обновляется только схема в метаданных; движки при чтении старых файлов возвращают `NULL`. |

---

# 🚀 Демонстрация ЛР5: Хранилище и витрина ClickHouse (Вариант 93)

> [!IMPORTANT]
> Для полной автоматической проверки ЛР5 запустите скрипт:
> ```bash
> bash demo_lab5.sh
> ```
> Либо выполняйте шаги ниже последовательно.

---

## Шаг 14. Проверка и запуск сервисов профиля lab5

**Что говоришь преподавателю:**  
*«Для ЛР5 поднят профиль lab5: аналитическое хранилище ClickHouse (сервис warehouse на портах 8123 и 9002) и инструмент визуализации Metabase (сервис bi на порту 8088), а также S3 и каталог таблиц.»*

### Команда:
```bash
docker compose --profile lab5 up -d
docker compose ps
```

### Ожидаемый вывод:
```text
NAME         IMAGE                                       COMMAND                  SERVICE      STATUS
bi           metabase/metabase:v0.63.16.7                "/app/run_metabase.…"   bi           Up (healthy)
catalog      quay.io/lakekeeper/catalog:v0.13.6          "/home/nonroot/lakek…"   catalog      Up (healthy)
catalog-db   postgres:17.11-alpine                       "docker-entrypoint.s…"   catalog-db   Up (healthy)
collector    bigdata-stand/collector:1                   "/usr/local/bin/entr…"   collector    Up
s3           chrislusf/seaweedfs:4.46                    "/entrypoint.sh serv…"   s3           Up (healthy)
warehouse    clickhouse/clickhouse-server:26.3.32.14-alpine "/entrypoint.sh"    warehouse    Up (healthy)
```

---

## Шаг 15. Проверка баз данных в ClickHouse (слои stg, core, mart)

**Что говоришь преподавателю:**  
*«Хранилище разделено на три концептуальных слоя: stg — слой посадки данных из S3, core — нормализованное ядро с историей SCD Type 2, mart — витрина 'звезда' для аналитики и BI.»*

### Команда:
```bash
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SHOW DATABASES LIKE '%stg%' OR name LIKE '%core%' OR name LIKE '%mart%';
"
```

### Ожидаемый вывод:
```text
core
mart
stg
```

---

## Шаг 16. Загрузка данных и проверка идемпотентности

**Что говоришь преподавателю:**  
*«Данные загружаются из S3 Parquet запросом jobs/stg_load.sql, а затем трансформируются через jobs/mart_load.sql. Повторная загрузка суток заменяет партицию через DROP PARTITION и не создает дубликатов строк.»*

### Команда:
```bash
# 1. Загрузка всех суток
bash jobs/load_all_lab5.sh

# 2. Проверка идемпотентности (повторная загрузка одних суток)
docker compose exec -T warehouse clickhouse-client -u student --password student --param_day='2026-10-08' < jobs/stg_load.sql
docker compose exec -T warehouse clickhouse-client -u student --password student --param_day='2026-10-08' < jobs/mart_load.sql

# 3. Число строк не изменилось
docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT count(*) FROM stg.events;"
```

---

## Шаг 17. Проверка витрины «Звезда» и материализованного представления

**Что говоришь преподавателю:**  
*«Витрина спроектирована по схеме 'звезда': таблица фактов mart.fact_events (зернистость — 1 событие) и измерения mart.dim_type, mart.dim_actor, mart.dim_repo. Почасовые агрегаты для дашборда автоматически считает инкрементальное материализованное представление mart.mv_hourly_events на SummingMergeTree.»*

### Команда:
```bash
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT '1. Факты (mart.fact_events):' AS entity, count(*) AS cnt FROM mart.fact_events
UNION ALL
SELECT '2. Почасовой агрегат (mart.agg_hourly_events):', count(*) FROM mart.agg_hourly_events
UNION ALL
SELECT '3. Измерение типов (mart.dim_type):', count(*) FROM mart.dim_type
UNION ALL
SELECT '4. Измерение пользователей (mart.dim_actor):', count(*) FROM mart.dim_actor
UNION ALL
SELECT '5. Измерение репозиториев (mart.dim_repo):', count(*) FROM mart.dim_repo;
"
```

---

## Шаг 18. Проверка SCD Type 2: запрос «Как выглядели данные на дату»

**Что говоришь преподавателю:**  
*«Для репозиториев ведется история изменения имени и класса суточной активности (low/medium/high). Запрос на историческую дату 2026-10-10 возвращает версию строки, действовавшую на тот момент, а запрос на текущую дату — актуальное состояние.»*

### Команда:
```bash
# Пример запроса для репозитория:
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT
    repo_id,
    repo_name,
    activity_class,
    daily_events,
    valid_from,
    valid_to,
    is_current
FROM mart.dim_repo
WHERE repo_id = 868350162
ORDER BY valid_from;
"
```

---

## Шаг 19. Бенчмарк 3 путей к ответу на 1 аналитический вопрос

**Что говоришь преподавателю:**  
*«Один и тот же вопрос ('Число событий CreateEvent по суткам') решен тремя путями: через файлы S3 Parquet в DuckDB, через таблицу Iceberg в DuckDB и через витрину в ClickHouse. Ответы полностью совпадают, а ClickHouse выполняет запрос за считанные миллисекунды благодаря SummingMergeTree.»*

### Команда:
```bash
bash jobs/benchmark_3_paths.sh
```

---

## Шаг 20. Дополнительное задание (Бонус): 10x масштабирование и прогноз на 1000x

**Что говоришь преподавателю:**  
*«Мы создали изолированную тестовую таблицу с 10-кратным увеличением объема суток, измерили рост времени запроса и рассчитали экстраполяцию при росте на 3 порядка (1000x). Для S3 время деградирует из-за сетевого листинга файлов, в Iceberg время растет логарифмически за счет манифестов, а в ClickHouse с материализованным представлением время ответа практически константно O(1). Тестовые таблицы удалены.»*

### Команда:
```bash
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT 'Проверка отсутствия тестовых таблиц:' AS status, count(*) 
FROM system.tables 
WHERE database = 'mart' AND name LIKE '%scale_test%';
"
```
*(Результат: 0 — среда чиста)*

---

## Шаг 21. Проверка дашборда в Metabase

**Что говоришь преподавателю:**  
*«Веб-интерфейс Metabase доступен по адресу http://localhost:8088 (или с хоста http://192.168.64.8:8088). В нем настроены три графика по правилам оформления (Area Chart почасовой динамики, Horizontal Bar Chart топ-10 репозиториев, Donut Chart структуры типов событий).»*

### Вопросы и ответы для защиты ЛР5:

| Вопрос преподавателя | Ответ студента |
|---|---|
| **Зачем нужны три слоя stg, core, mart?** | `stg` обеспечивает быстрый буфер приёма данных из S3 без блокировок. `core` хранит выверенные данные и историчность сущностей (SCD2). `mart` содержит денормализованные витрины «звезда», оптимизированные под аналитические запросы и BI. |
| **Почему для агрегата выбран SummingMergeTree?** | `SummingMergeTree` автоматически выполняет предагрегацию числовых колонок при фоновых слияниях. Материализованное представление наполняет его на лету при вставке фактов, снижая время запроса с секунд до 3-5 мс. |
| **Как реализована идемпотентность загрузки суток?** | Перед вставкой выполняется `ALTER TABLE ... DROP PARTITION {day:Date}`. Повторный запуск полностью заменяет данные указанных суток, не создавая дубликатов строк. |
| **Как устроен SCD Type 2 в ClickHouse?** | При изменении атрибута закрывается старая версия строки (`valid_to = day - 1, is_current = 0`) и вставляется новая строка (`valid_from = day, valid_to = 9999-12-31, is_current = 1`). Запрос `WHERE date BETWEEN valid_from AND valid_to` мгновенно возвращает состояние сущности на любую дату. |
| **Почему витрина ClickHouse быстрее S3 Parquet в 40+ раз?** | 1) Отсутствуют накладные расходы на HTTP-запросы и чтение футеров. 2) Данные уже сжаты columnar-кодеками на локальном диске с кэшем засечек. 3) Материализованное представление сканирует всего сотни свернутых строк вместо сотен тысяч сырых событий. |


