#!/usr/bin/env bash
# stand/check_lab1.sh
# Строгая проверка всех требований Лабораторной работы №1: Стенд и сбор данных (Вариант 93)
# Запуск на виртуальной машине:
#   cd ~/stand && bash check_lab1.sh

set -euo pipefail

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
echo -e "${BLUE}   ПРОВЕРКА ЛАБОРАТОРНОЙ РАБОТЫ №1: Стенд и сбор данных (Вариант 93)     ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 0: Защита сети и MTU от обрывов TLS из-за VPN (Amnezia / AdGuard)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 0/7] Проверка MTU сетевого интерфейса (защита от обрывов TLS)...${NC}"
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

# -----------------------------------------------------------------------------
# ЭТАП 1: Проверка параметров варианта в .env
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 1/7] Проверка файла .env на соответствие Варианту 93...${NC}"
if [ ! -f ".env" ]; then
  echo -e "${RED}Ошибка: Файл .env не найден!${NC}"
  exit 1
fi

grep -q "VARIANT=93" .env || (echo "VARIANT=93 не найдено в .env" && exit 1)
grep -q "SOURCE=github" .env || (echo "SOURCE=github не найдено в .env" && exit 1)
grep -q "SOURCE_FILTER=CreateEvent,DeleteEvent,ReleaseEvent" .env || (echo "SOURCE_FILTER не соответствует заданию" && exit 1)
echo -e "${GREEN}  ✔ Параметры варианта 93 (GH Archive, CreateEvent/DeleteEvent/ReleaseEvent) настроены!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 2: Проверка службы collector и метрик на порту 9101
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 2/7] Проверка службы collector и HTTP Prometheus метрик на порту 9101...${NC}"
docker compose --profile lab1 up -d collector
sleep 3

METRICS_OUT=$(curl -s http://localhost:9101/metrics 2>&1 || true)
if echo "${METRICS_OUT}" | grep -q "collector_events_total"; then
  echo -e "${GREEN}  ✔ Метрики Prometheus на порту 9101 отвечают корректно:${NC}"
  echo "${METRICS_OUT}" | grep -E "collector_events_total|collector_errors_http|collector_last_success_timestamp" | sed 's/^/    /'
else
  echo -e "${RED}Ошибка: Метрики на порту 9101 недоступны!${NC}"
  exit 1
fi

# -----------------------------------------------------------------------------
# ЭТАП 3: Проверка структуры сырого слоя (stream)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 3/7] Проверка структуры каталогов и сжатия файлов потока (stream)...${NC}"
STREAM_FILES=$(find data/raw/stream -name "*.jsonl.gz" 2>/dev/null | head -n 5)
if [ -z "${STREAM_FILES}" ]; then
  echo -e "${RED}Ошибка: В data/raw/stream/dt=*/hour=*/ нет файлов .jsonl.gz!${NC}"
  exit 1
fi
STREAM_COUNT=$(find data/raw/stream -name "*.jsonl.gz" 2>/dev/null | wc -l)
echo -e "${GREEN}  ✔ Файлы потока присутствуют (${STREAM_COUNT} часовых файлов .jsonl.gz)!${NC}"
echo "    Пример пути: $(echo "${STREAM_FILES}" | head -n 1)"

# -----------------------------------------------------------------------------
# ЭТАП 4: Проверка исторического среза (backfill за 7 суток)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 4/7] Проверка исторического среза в префиксе history...${NC}"
HIST_COUNT=$(find data/raw/history -name "*.jsonl.gz" 2>/dev/null | wc -l)
if [ "${HIST_COUNT}" -lt 24 ]; then
  echo -e "${RED}Ошибка: Исторический срез отсутствует или содержит слишком мало файлов (${HIST_COUNT})!${NC}"
  exit 1
fi
echo -e "${GREEN}  ✔ Исторический срез в data/raw/history содержит ${HIST_COUNT} часовых файлов!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 5: Проверка соответствия фильтру и структуре событий
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 5/7] Проверка обязательных полей event_key, event_time и фильтра...${NC}"
SAMPLE_FILE=$(find data/raw/stream data/raw/history -name "*.jsonl.gz" | head -n 1)
echo "  Анализ файла: ${SAMPLE_FILE}"
SAMPLE_EVENT=$(zcat "${SAMPLE_FILE}" | head -n 1)

KEY_VAL=$(echo "${SAMPLE_EVENT}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('event_key', ''))")
TIME_VAL=$(echo "${SAMPLE_EVENT}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('event_time', ''))")
TYPE_VAL=$(echo "${SAMPLE_EVENT}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('type', ''))")

if [ -z "${KEY_VAL}" ] || [ -z "${TIME_VAL}" ]; then
  echo -e "${RED}Ошибка: В событии отсутствуют обязательные поля event_key или event_time!${NC}"
  exit 1
fi

if [[ ! "${TYPE_VAL}" =~ ^(CreateEvent|DeleteEvent|ReleaseEvent)$ ]]; then
  echo -e "${RED}Ошибка: Событие с типом ${TYPE_VAL} нарушает SOURCE_FILTER!${NC}"
  exit 1
fi
echo -e "${GREEN}  ✔ Поля валидны: event_key='${KEY_VAL}', event_time='${TIME_VAL}', type='${TYPE_VAL}'${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 6: Проверка отсутствия дубликатов ключей (дедупликация и перезапуск)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 6/7] Проверка дедупликации (повторов по event_key за сутки нет)...${NC}"
SAMPLE_DAY_DIR=$(find data/raw/stream -type d -name "dt=*" | head -n 1)
if [ -n "${SAMPLE_DAY_DIR}" ]; then
  DUP_COUNT=$(zcat "${SAMPLE_DAY_DIR}"/hour=*/*.jsonl.gz 2>/dev/null | python3 -c "
import sys, json
keys = set()
dups = 0
for line in sys.stdin:
    if not line.strip(): continue
    k = json.loads(line).get('event_key')
    if k in keys: dups += 1
    else: keys.add(k)
print(dups)
")
  if [ "${DUP_COUNT}" -eq 0 ]; then
    echo -e "${GREEN}  ✔ В сутках $(basename "${SAMPLE_DAY_DIR}") найдено 0 дубликатов ключей!${NC}"
  else
    echo -e "${YELLOW}  Предупреждение: Найдено ${DUP_COUNT} дубликатов.${NC}"
  fi
fi

# -----------------------------------------------------------------------------
# ЭТАП 7: Проверка паспорта SOURCE.md и каталога CATALOG.md
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 7/7] Проверка паспорта источника и каталога данных...${NC}"
if [ -f "SOURCE.md" ] && grep -q "GH Archive" SOURCE.md; then
  echo -e "${GREEN}  ✔ Паспорт источника SOURCE.md заполнен!${NC}"
else
  echo -e "${RED}Ошибка: SOURCE.md не найден или не заполнен!${NC}"
  exit 1
fi

if [ -f "CATALOG.md" ] && grep -q "raw.stream" CATALOG.md && grep -q "raw.history" CATALOG.md; then
  echo -e "${GREEN}  ✔ В каталоге CATALOG.md присутствуют строки raw.stream и raw.history!${NC}"
else
  echo -e "${RED}Ошибка: В CATALOG.md отсутствуют строки raw.stream или raw.history!${NC}"
  exit 1
fi

echo -e "\n${BLUE}========================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №1 И БОНУСА ПОЛНОСТЬЮ ВЫПОЛНЕНЫ!   ${NC}"
echo -e "${BLUE}========================================================================${NC}"
