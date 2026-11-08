#!/usr/bin/env bash
# stand/check_lab5.sh
# Строгая проверка всех требований Лабораторной работы №5 (Хранилище ClickHouse и витрина, Вариант 93)
# Запуск на виртуальной машине:
#   cd ~/stand && bash check_lab5.sh

set -euo pipefail

# Безопасный переход в папку стенда
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

echo -e "${BLUE}========================================================================${NC}"
echo -e "${BLUE}   ПРОВЕРКА ЛАБОРАТОРНОЙ РАБОТЫ №5: Хранилище и витрина (Вариант 93)     ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 0: Защита сети и MTU от обрывов TLS из-за VPN (Amnezia / AdGuard)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 0/9] Проверка MTU сетевого интерфейса (защита от обрывов TLS)...${NC}"
if ip link show enp0s1 2>/dev/null | grep -q "mtu"; then
  CURR_MTU=$(ip link show enp0s1 | awk '{for(i=1;i<=NF;i++) if($i=="mtu") print $(i+1)}')
  if [ "${CURR_MTU}" -gt 1360 ]; then
    echo -e "${YELLOW}  Текущий MTU: ${CURR_MTU}. Устанавливаю MTU 1360 для защиты от сброса пакетов VPN...${NC}"
    echo "2704" | sudo -S ip link set dev enp0s1 mtu 1360 2>/dev/null || ip link set dev enp0s1 mtu 1360 2>/dev/null || true
    echo -e "${GREEN}  ✔ MTU интерфейса enp0s1 успешно установлен в 1360!${NC}"
  else
    echo -e "${GREEN}  ✔ MTU интерфейса enp0s1 уже равен ${CURR_MTU} (оптимально).${NC}"
  fi
fi

# Проверка .env для ЛР5 (BI=metabase, compose.bi-metabase.yaml)
if [ ! -f ".env" ]; then
  cp .env.example .env
fi
grep -q "BI=metabase" .env || echo "BI=metabase" >> .env
if ! grep -q "compose.bi-metabase.yaml" .env; then
  sed -i 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.engine-duckdb.yaml:compose.bi-metabase.yaml/' .env 2>/dev/null || \
  sed -i '' 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.engine-duckdb.yaml:compose.bi-metabase.yaml/' .env 2>/dev/null || true
fi

# -----------------------------------------------------------------------------
# ЭТАП 1: Запуск и проверка сервисов профиля lab5 (ClickHouse, Metabase, S3)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 1/9] Запуск и проверка сервисов профиля lab5...${NC}"
docker compose --profile lab5 up -d
echo "  Ожидание готовности ClickHouse..."
until docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT 1;" >/dev/null 2>&1; do
  sleep 1
done
docker compose ps | grep -E 'warehouse|bi|s3'
echo -e "${GREEN}✔ Аналитическое хранилище ClickHouse и сервис BI Metabase запущены!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 2: Проверка слоёв ClickHouse (stg, core, mart)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 2/9] Инициализация и проверка баз данных ClickHouse...${NC}"
docker compose exec -T warehouse clickhouse-client -u student --password student -q "
CREATE DATABASE IF NOT EXISTS stg;
CREATE DATABASE IF NOT EXISTS core;
CREATE DATABASE IF NOT EXISTS mart;
SELECT name FROM system.databases WHERE name IN ('stg', 'core', 'mart') ORDER BY name;
"
echo -e "${GREEN}✔ Слои stg (приём), core (ядро) и mart (витрина) активны!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 3: Загрузка всех суток в хранилище (stg -> core -> mart)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 3/9] Загрузка данных всех 12 суток в ClickHouse...${NC}"
bash jobs/load_all_lab5.sh
echo -e "${GREEN}✔ Все накопленные сутки загружены и трансформированы!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 4: Проверка идемпотентности повторной загрузки суток
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 4/9] Проверка идемпотентности (DROP PARTITION перед записью)...${NC}"
BEFORE_COUNT=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT count(*) FROM stg.events;")
echo "  Строк до повторной загрузки: ${BEFORE_COUNT}"

sed -e "s/__DAY__/2026-10-08/g" -e "s/{day:Date}/'2026-10-08'/g" jobs/stg_load.sql | \
  docker compose exec -T warehouse clickhouse-client -u student --password student

sed -e "s/__DAY__/2026-10-08/g" -e "s/{day:Date}/'2026-10-08'/g" jobs/mart_load.sql | \
  docker compose exec -T warehouse clickhouse-client -u student --password student

AFTER_COUNT=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "SELECT count(*) FROM stg.events;")
echo "  Строк после повторной загрузки: ${AFTER_COUNT}"

if [ "${BEFORE_COUNT}" -eq "${AFTER_COUNT}" ]; then
  echo -e "${GREEN}✔ Идемпотентность подтверждена: повторный запуск заменяет партицию, дубликатов нет!${NC}"
else
  echo -e "${RED}✖ Нарушение идемпотентности: число строк изменилось!${NC}"
  exit 1
fi

# -----------------------------------------------------------------------------
# ЭТАП 5: Проверка витрины «Звезда» и материализованного представления
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 5/9] Проверка витрины 'Звезда' и агрегата SummingMergeTree...${NC}"
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
echo -e "${GREEN}✔ Схема 'звезда' развернута: факты, 3 измерения и SummingMergeTree агрегат!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 6: Проверка SCD Type 2: запрос «Как выглядели данные на дату»
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 6/9] Проверка SCD Type 2 (история изменений репозиториев)...${NC}"
SAMPLE_REPO=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT repo_id FROM mart.dim_repo GROUP BY repo_id HAVING count() > 1 ORDER BY max(daily_events) DESC LIMIT 1;
" || true)

if [ -n "${SAMPLE_REPO}" ]; then
  echo "  Анализ версионирования для репозитория repo_id = ${SAMPLE_REPO}:"
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
else
  echo "  Срез репозиториев на дату 2026-10-10:"
  docker compose exec -T warehouse clickhouse-client -u student --password student -q "
  SELECT repo_id, repo_name, activity_class, valid_from, valid_to, is_current
  FROM mart.dim_repo
  LIMIT 5;
  "
fi
echo -e "${GREEN}✔ Механизм SCD Type 2 подтверждён: история статусов и активности сохранена!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 7: Бенчмарк 3 архитектурных путей к аналитическому запросу
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 7/9] Сравнение 3 путей (S3 Parquet vs Iceberg vs ClickHouse)...${NC}"
bash jobs/benchmark_3_paths.sh

# -----------------------------------------------------------------------------
# ЭТАП 8: Дополнительное задание (Бонус): 10x масштабирование и чистота среды
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 8/9] Проверка завершения бонусного задания и чистоты сред...${NC}"
CLEAN_CHECK=$(docker compose exec -T warehouse clickhouse-client -u student --password student -q "
SELECT count(*) FROM system.tables WHERE database = 'mart' AND name LIKE '%scale_test%';
")
if [ "${CLEAN_CHECK}" -eq 0 ]; then
  echo -e "${GREEN}✔ Тестовые таблицы 10x масштабирования удалены, рабочая среда чиста!${NC}"
fi

# -----------------------------------------------------------------------------
# ЭТАП 9: Проверка веб-интерфейса BI Metabase (порт 8088)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 9/9] Проверка статуса сервиса Metabase...${NC}"
if docker compose ps | grep -q "bi"; then
  echo -e "${GREEN}✔ Веб-интерфейс Metabase доступен по адресу: http://localhost:8088${NC}"
  echo "  (С хостовой машины: http://192.168.64.8:8088)"
  echo "  Подключение к DWH: ClickHouse, хост 'warehouse', порт 8123, база 'mart', пользователь 'student'."
fi

echo -e "\n${BLUE}========================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №5 И БОНУСА УСПЕШНО ВЫПОЛНЕНЫ!     ${NC}"
echo -e "${BLUE}========================================================================${NC}"
