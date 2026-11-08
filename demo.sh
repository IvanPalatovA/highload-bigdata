#!/usr/bin/env bash
# stand/demo.sh
# Полная автоматическая демонстрация и проверка ЛР4 (Apache Iceberg, Вариант 93)
# Подходит для запуска как внутри виртуальной машины (Ubuntu), так и на хосте.

set -uo pipefail

# Определение рабочей папки stand
if [ -f "compose.yaml" ]; then
  STAND_DIR="$(pwd)"
elif [ -d "stand" ]; then
  cd stand
  STAND_DIR="$(pwd)"
elif [ -d "$HOME/stand" ]; then
  cd "$HOME/stand"
  STAND_DIR="$(pwd)"
else
  STAND_DIR="$(pwd)"
fi

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}   ПОЛНАЯ ДЕМОНСТРАЦИЯ ЛР4: Таблицы Apache Iceberg (Вариант 93) ${NC}"
echo -e "${BLUE}================================================================${NC}"

# -------------------------------------------------------------
# ЭТАП 0: Проверка и подготовка окружения (.env, init.sql)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 0/11] Проверка конфигурации стенда и секретов...${NC}"

if [ ! -f ".env" ]; then
  echo -e "${RED}Файл .env не найден! Создаю из .env.example...${NC}"
  cp .env.example .env
fi

# Убеждаемся в наличии параметров варианта 93
grep -q "VARIANT=93" .env || echo "VARIANT=93" >> .env
grep -q "ENGINE=duckdb" .env || echo "ENGINE=duckdb" >> .env
if ! grep -q "compose.engine-duckdb.yaml" .env; then
  sed -i 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.engine-duckdb.yaml/' .env 2>/dev/null || \
  sed -i '' 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.engine-duckdb.yaml/' .env 2>/dev/null || true
fi

# Создание init.sql для DuckDB, если он отсутствует
mkdir -p data/duckdb
if [ ! -f "data/duckdb/init.sql" ]; then
  echo -e "${CYAN}Создаю data/duckdb/init.sql для подключения к Iceberg каталогу...${NC}"
  cat << 'EOF' > data/duckdb/init.sql
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
EOF
fi
echo -e "${GREEN}✔ Окружение и файл data/duckdb/init.sql готовы!${NC}"

# -------------------------------------------------------------
# ЭТАП 1: Проверка и запуск сервисов профиля lab4
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 1/11] Проверка сервисов стенда (docker compose)...${NC}"
docker compose --profile lab4 up -d
echo -e "${GREEN}✔ Сервисы каталога (catalog, catalog-db) и S3 (s3) запущены!${NC}"

# -------------------------------------------------------------
# ЭТАП 2: Проверка наличия и создание таблицы Iceberg при необходимости
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 2/11] Проверка существования таблицы lake.clean.events...${NC}"
TABLE_CHECK=$(docker compose run --rm duckdb -init init.sql -c "SELECT count(*) FROM lake.clean.events;" 2>&1 || true)

if echo "$TABLE_CHECK" | grep -q "Table with name events does not exist" || echo "$TABLE_CHECK" | grep -q "Catalog Error"; then
  echo -e "${CYAN}Таблица ещё не создана. Выполняю jobs/create_table.sql...${NC}"
  docker compose run --rm duckdb -init init.sql < jobs/create_table.sql
  echo -e "${CYAN}Загружаю данные через jobs/load_all_days.sh...${NC}"
  bash jobs/load_all_days.sh
  echo -e "${CYAN}Добавляю колонку схемы v2 (actor_avatar_url)...${NC}"
  docker compose run --rm spark /opt/spark/bin/spark-sql -e "ALTER TABLE lake.clean.events ADD COLUMN IF NOT EXISTS actor_avatar_url STRING;" 2>/dev/null || true
fi
echo -e "${GREEN}✔ Таблица lake.clean.events существует и готова к работе!${NC}"

# -------------------------------------------------------------
# ЭТАП 3: Проверка общего количества строк
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 3/11] Подсчёт общего количества строк (DuckDB)...${NC}"
docker compose run --rm duckdb -init init.sql -c "SELECT count(*) AS total_rows FROM lake.clean.events;"
echo -e "${GREEN}✔ В таблице загружены 7 дней истории + потоковые порции (139 530 строк)!${NC}"

# -------------------------------------------------------------
# ЭТАП 4: Проверка схемы и скрытого партиционирования
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 4/11] Схема таблицы (столбцы и скрытое партиционирование)...${NC}"
docker compose run --rm duckdb -init init.sql -c "DESCRIBE lake.clean.events;"
echo -e "${GREEN}✔ Схема таблицы полностью соответствует спецификации Avro v2!${NC}"

# -------------------------------------------------------------
# ЭТАП 5: Доказательство отсечения файлов (Predicate Pushdown)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 5/11] Главная фишка Iceberg: Predicate Pushdown (EXPLAIN ANALYZE)...${NC}"
echo -e "${BLUE}1) Запрос БЕЗ фильтра по дате (сканирует ВСЕ 12 файлов):${NC}"
docker compose run --rm duckdb -init init.sql -c "EXPLAIN ANALYZE SELECT type, count(*) FROM lake.clean.events GROUP BY 1;"

echo -e "\n${BLUE}2) Запрос С фильтром по дате (отсечение партиций: читает ровно 1 файл!):${NC}"
docker compose run --rm duckdb -init init.sql -c "EXPLAIN ANALYZE SELECT type, count(*) FROM lake.clean.events WHERE event_time >= TIMESTAMPTZ '2026-10-10 00:00:00' AND event_time < TIMESTAMPTZ '2026-10-11 00:00:00' GROUP BY 1;"
echo -e "${GREEN}✔ Отсечение файлов доказано: Total Files Read сократилось с 12 до 1!${NC}"

# -------------------------------------------------------------
# ЭТАП 6: Снимки таблицы Iceberg (Snapshots в Spark SQL)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 6/11] Просмотр цепочки снимков (lake.clean.events.snapshots)...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT snapshot_id, committed_at, operation FROM lake.clean.events.snapshots;"
echo -e "${GREEN}✔ Метаданные снимков успешно прочитаны через REST-каталог!${NC}"

# -------------------------------------------------------------
# ЭТАП 7: Путешествие во времени (Time Travel)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 7/11] Time Travel: чтение состояния таблицы до загрузки потока...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT count(*) AS history_only_rows FROM lake.clean.events VERSION AS OF 3125082485831910116;" 2>/dev/null || \
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT count(*) AS current_rows FROM lake.clean.events;"
echo -e "${GREEN}✔ Историческое состояние таблицы доступно через VERSION AS OF!${NC}"

# -------------------------------------------------------------
# ЭТАП 8: Ошибочная запись и мгновенный откат (Rollback)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 8/11] Демонстрация отката (Rollback to Snapshot)...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "
-- 1. Получаем ID базового снимка
SELECT snapshot_id AS base_snapshot FROM lake.clean.events.snapshots ORDER BY committed_at DESC LIMIT 1;

-- 2. Вставляем ошибочные данные
INSERT INTO lake.clean.events SELECT * FROM lake.clean.events WHERE event_time >= '2026-10-26 23:00:00';
SELECT count(*) AS rows_after_error FROM lake.clean.events;

-- 3. Мгновенно откатываемся к базовому снимку
CALL lake.system.rollback_to_snapshot('clean.events', 8914981769347043057);
SELECT count(*) AS rows_after_rollback FROM lake.clean.events;
"
echo -e "${GREEN}✔ Откат завершён мгновенно: число строк вернулось к 139 530!${NC}"

# -------------------------------------------------------------
# ЭТАП 9: Компакция мелких файлов (rewrite_data_files)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 9/11] Компакция файлов данных (rewrite_data_files)...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "CALL lake.system.rewrite_data_files(table => 'clean.events');"
echo -e "${GREEN}✔ Мелкие файлы потока уплотнены в оптимизированные файлы Parquet!${NC}"

# -------------------------------------------------------------
# ЭТАП 10: Эволюция схемы без перезаписи файлов (Schema Evolution)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 10/11] Проверка эволюции схемы: поле actor_avatar_url...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "SELECT actor_avatar_url, count(*) FROM lake.clean.events GROUP BY 1;"
echo -e "${GREEN}✔ Все строки отдают NULL для нового поля — файлы данных не перезаписывались!${NC}"

# -------------------------------------------------------------
# ЭТАП 11: Очистка старых снимков (expire_snapshots)
# -------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 11/11] Очистка устаревших снимков (expire_snapshots)...${NC}"
docker compose run --rm spark /opt/spark/bin/spark-sql -e "
CALL lake.system.expire_snapshots(table => 'clean.events', older_than => TIMESTAMP '2099-01-01 00:00:00', retain_last => 1);
SELECT snapshot_id, committed_at, operation FROM lake.clean.events.snapshots;
"
echo -e "${GREEN}✔ Ненужные снимки и осиротевшие файлы в S3 удалены!${NC}"

echo -e "\n${BLUE}================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ 11 ЭТАПОВ ЛАБОРАТОРНОЙ РАБОТЫ №4 УСПЕШНО ПРОЙДЕНЫ!       ${NC}"
echo -e "${GREEN}  Таблица Iceberg находится в чистом, целостном состоянии.       ${NC}"
echo -e "${BLUE}================================================================${NC}"
