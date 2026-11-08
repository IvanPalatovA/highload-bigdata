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

