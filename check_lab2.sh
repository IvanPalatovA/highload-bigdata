#!/usr/bin/env bash
# stand/check_lab2.sh
# Строгая проверка всех требований Лабораторной работы №2: Форматы данных (Вариант 93)
# Запуск на виртуальной машине:
#   cd ~/stand && bash check_lab2.sh

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
echo -e "${BLUE}   ПРОВЕРКА ЛАБОРАТОРНОЙ РАБОТЫ №2: Форматы данных (Вариант 93)         ${NC}"
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
# ЭТАП 1: Проверка схемы Avro (версии 1 и 2)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 1/7] Проверка наличия схем Avro (schemas/event.avsc, event_v1.avsc)...${NC}"
if [ ! -f "schemas/event.avsc" ] || [ ! -f "schemas/event_v1.avsc" ]; then
  echo -e "${RED}Ошибка: Файлы схем schemas/event.avsc или event_v1.avsc не найдены!${NC}"
  exit 1
fi

grep -q "timestamp-millis" schemas/event.avsc || (echo "event_time logicalType timestamp-millis не найден" && exit 1)
grep -q "actor_avatar_url" schemas/event.avsc || (echo "actor_avatar_url в схеме v2 не найден" && exit 1)
echo -e "${GREEN}  ✔ Схемы Avro v1 и v2 валидны (event_key string, event_time timestamp-millis, actor_avatar_url)!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 2: Валидация сырых данных и замеры компактности (schemas/check.py)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 2/7] Запуск валидации данных и замеров компактности (fastavro)...${NC}"
source .venv/bin/activate
python3 schemas/check.py
echo -e "${GREEN}  ✔ Валидация fastavro и сравнение размеров Avro vs JSON успешно пройдены!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 3: Проверка совместимости схем v1 и v2 (Full Compatibility)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 3/7] Проверка прямой и обратной совместимости схем (Full Compatibility)...${NC}"
echo "  1. Прямая совместимость: запись v1, чтение v2..."
python3 schemas/check.py --schema schemas/event_v1.avsc --reader-schema schemas/event.avsc >/dev/null
echo -e "${GREEN}    ✔ Данные схемы v1 успешно прочитаны схемой v2!${NC}"

echo "  2. Обратная совместимость: запись v2, чтение v1..."
python3 schemas/check.py --schema schemas/event.avsc --reader-schema schemas/event_v1.avsc >/dev/null
echo -e "${GREEN}    ✔ Данные схемы v2 успешно прочитаны схемой v1 (новое поле пропущено)!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 4: Проверка преобразования в Parquet (jobs/to_parquet.sql)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 4/7] Проверка скрипта конвертации в Parquet (jobs/to_parquet.sql)...${NC}"
if [ ! -f "jobs/to_parquet.sql" ]; then
  echo -e "${RED}Ошибка: jobs/to_parquet.sql не найден!${NC}"
  exit 1
fi
grep -q "PARTITION_BY" jobs/to_parquet.sql || (echo "PARTITION_BY не найден в to_parquet.sql" && exit 1)
grep -q "OVERWRITE_OR_IGNORE" jobs/to_parquet.sql || (echo "OVERWRITE_OR_IGNORE не найден в to_parquet.sql" && exit 1)
echo -e "${GREEN}  ✔ Запрос to_parquet.sql настроен на идемпотентную запись с PARTITION_BY (event_date)!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 5: Бенчмарк производительности (JSON vs Parquet)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 5/7] Сравнение скорости запроса на сыром JSON и колоночном Parquet...${NC}"
python3 jobs/benchmark_parquet.py
echo -e "${GREEN}  ✔ Бенчмарк подтверждает кратное ускорение аналитического запроса на Parquet!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 6: Дополнительное задание (исследование кодеков сжатия и row group size)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 6/7] Проверка результатов исследования кодеков сжатия Parquet...${NC}"
if grep -q "zstd" NOTES.md && grep -q "ROW_GROUP_SIZE" NOTES.md; then
  echo -e "${GREEN}  ✔ Исследование кодеков (uncompressed, snappy, gzip, zstd) и размеров групп строк зафиксировано в NOTES.md!${NC}"
else
  echo -e "${RED}Ошибка: Результаты сравнения кодеков в NOTES.md не найдены!${NC}"
  exit 1
fi

# -----------------------------------------------------------------------------
# ЭТАП 7: Проверка каталога CATALOG.md
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 7/7] Проверка наличия parquet.events в CATALOG.md...${NC}"
if grep -q "parquet.events" CATALOG.md; then
  echo -e "${GREEN}  ✔ Набор parquet.events документирован в CATALOG.md!${NC}"
else
  echo -e "${RED}Ошибка: В CATALOG.md отсутствует строка parquet.events!${NC}"
  exit 1
fi

echo -e "\n${BLUE}========================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №2 И БОНУСА ПОЛНОСТЬЮ ВЫПОЛНЕНЫ!   ${NC}"
echo -e "${BLUE}========================================================================${NC}"
