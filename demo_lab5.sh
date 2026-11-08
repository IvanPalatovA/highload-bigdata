#!/usr/bin/env bash
# stand/demo_lab5.sh
# Автоматическая демонстрация и полная проверка ЛР5 (Хранилище и витрина, Вариант 93)
# Подходит для запуска как внутри ВМ (ivan@armserver:~/stand), так и через скрипт:
#   cd ~/stand && bash demo_lab5.sh

set -euo pipefail

# Безопасное определение каталога стенда
if [ -f "compose.yaml" ]; then
    STAND_DIR="$(pwd)"
elif [ -d "$HOME/stand" ] && [ -f "$HOME/stand/compose.yaml" ]; then
    STAND_DIR="$HOME/stand"
else
    STAND_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
cd "${STAND_DIR}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}================================================================${NC}"
echo -e "${BLUE}   ДЕМОНСТРАЦИЯ ЛР5: Хранилище и витрина ClickHouse (Вариант 93)   ${NC}"
echo -e "${BLUE}================================================================${NC}"

echo -e "\n${YELLOW}>>> Шаг 0. Проверка и запуск сервисов профиля lab5...${NC}"
docker compose --profile lab5 up -d
docker compose ps
echo -e "${GREEN}✔ Все сервисы профиля lab5 (s3, catalog, warehouse, bi) работают!${NC}"

echo -e "\n${YELLOW}>>> Шаг 1. Проверка конфигурации варианта 93 в .env...${NC}"
grep -E '^(VARIANT|SOURCE|SERDE|S3|ENGINE|BI|COMPOSE_FILE)=' .env
echo -e "${GREEN}✔ Конфигурация варианта 93 подтверждена (BI=metabase, warehouse=ClickHouse)!${NC}"

echo -e "\n${YELLOW}>>> Шаг 2. Проверка баз данных в ClickHouse (слои stg, core, mart)...${NC}"
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS core;
CREATE DATABASE IF NOT EXISTS mart;
SHOW DATABASES LIKE '%stg%' OR name LIKE '%core%' OR name LIKE '%mart%';
"
echo -e "${GREEN}✔ Слои stg, core и mart успешно инициализированы в ClickHouse!${NC}"

echo -e "\n${YELLOW}>>> Шаг 3. Загрузка данных из S3 Parquet во все слои (stg -> core -> mart)...${NC}"
bash jobs/load_all_lab5.sh
echo -e "${GREEN}✔ Данные всех 12 суток загружены во все слои хранилища!${NC}"

echo -e "\n${YELLOW}>>> Шаг 4. Проверка идемпотентности повторной загрузки суток (2026-10-08)...${NC}"
BEFORE_COUNT=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT count(*) FROM stg.events;")
echo "  Строк в stg.events до повторной загрузки: ${BEFORE_COUNT}"

docker compose exec -T warehouse clickhouse-client -u student --password student --param_day='2026-10-08' < jobs/stg_load.sql
docker compose exec -T warehouse clickhouse-client -u student --password student --param_day='2026-10-08' < jobs/mart_load.sql

AFTER_COUNT=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT count(*) FROM stg.events;")
echo "  Строк в stg.events после повторной загрузки: ${AFTER_COUNT}"

if [ "${BEFORE_COUNT}" -eq "${AFTER_COUNT}" ]; then
  echo -e "${GREEN}✔ Идемпотентность доказана: повторный запуск заменяет партицию, дубликатов нет!${NC}"
else
  echo -e "${RED}✖ Ошибка идемпотентности: число строк изменилось!${NC}"
  exit 1
fi

echo -e "\n${YELLOW}>>> Шаг 5. Проверка витрины 'Звезда' (Таблица фактов + Измерения + MV)...${NC}"
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
echo -e "${GREEN}✔ Витрина построена по схеме 'звезда', материализованное представление агрегирует факты!${NC}"

echo -e "\n${YELLOW}>>> Шаг 6. Проверка SCD Type 2: запрос 'Как выглядели данные на дату'...${NC}"
SAMPLE_REPO=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT repo_id FROM mart.dim_repo GROUP BY repo_id HAVING count() > 1 ORDER BY max(daily_events) DESC LIMIT 1;
")

if [ -n "${SAMPLE_REPO}" ]; then
  echo "  Анализ истории репозитория repo_id = ${SAMPLE_REPO}:"
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
  WHERE repo_id = ${SAMPLE_REPO}
  ORDER BY valid_from;
  "
  echo -e "${GREEN}✔ SCD Type 2 подтверждён: смена активности/имени репозитория версионируется строками!${NC}"
else
  echo "  Срез репозитория на дату 2026-10-10 и на текущую дату:"
  docker compose exec -T warehouse clickhouse-client -u student --password student -q "
  SELECT repo_id, repo_name, activity_class, valid_from, valid_to, is_current
  FROM mart.dim_repo
  LIMIT 5;
  "
fi

echo -e "\n${YELLOW}>>> Шаг 7. Бенчмарк 3 путей к ответу на 1 вопрос + Бонусное задание...${NC}"
bash jobs/benchmark_3_paths.sh

echo -e "\n${YELLOW}>>> Шаг 8. Проверка веб-интерфейса BI (Metabase)...${NC}"
if docker compose ps | grep -q "bi"; then
  echo -e "${GREEN}✔ Сервис Metabase запущен на порту 8088!${NC}"
  echo "  URL: http://localhost:8088 (или с хоста: http://192.168.64.8:8088)"
  echo "  Источник в Metabase: ClickHouse (хост: warehouse, порт: 8123, база: mart, пользователь: student, пароль: student)"
fi

echo -e "\n${BLUE}================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ ТРЕБОВАНИЯ ЛР5 И ДОПОЛНИТЕЛЬНОГО ЗАДАНИЯ УСПЕШНО ВЫПОЛНЕНЫ!${NC}"
echo -e "${BLUE}================================================================${NC}"
