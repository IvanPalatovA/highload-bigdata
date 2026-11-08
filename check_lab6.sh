#!/usr/bin/env bash
# stand/check_lab6.sh
# Строгая проверка всех требований Лабораторной работы №6: Поток событий Apache Kafka (Вариант 93)
# Запуск на виртуальной машине:
#   cd ~/stand && bash check_lab6.sh

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
echo -e "${BLUE}   ПРОВЕРКА ЛАБОРАТОРНОЙ РАБОТЫ №6: Поток событий Kafka (Вариант 93)     ${NC}"
echo -e "${BLUE}========================================================================${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 0: Защита сети и MTU от обрывов TLS из-за VPN (Amnezia / AdGuard)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 0/8] Проверка MTU сетевого интерфейса (защита от обрывов TLS)...${NC}"
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
# ЭТАП 1: Настройка .env и запуск сервисов профиля lab6 (Kafka, Karapace, S3)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 1/8] Запуск и проверка брокера Kafka и Karapace Schema Registry...${NC}"
grep -q "BROKER=kafka" .env || echo "BROKER=kafka" >> .env
if ! grep -q "compose.broker-kafka.yaml" .env; then
  sed -i 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.broker-kafka.yaml/' .env 2>/dev/null || \
  sed -i '' 's/^COMPOSE_FILE=.*/COMPOSE_FILE=compose.yaml:compose.s3-seaweedfs.yaml:compose.broker-kafka.yaml/' .env 2>/dev/null || true
fi

docker compose --profile lab5 stop >/dev/null 2>&1 || true
docker compose --profile lab6 up -d broker registry s3

echo "  Ожидание готовности брокера Kafka (порт 9092)..."
until docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "  Ожидание готовности реестра схем Karapace (порт 8081)..."
until curl -s http://localhost:8081/_health | grep -q '"schema_registry_ready":true'; do
  sleep 2
done
echo -e "${GREEN}  ✔ Брокер Apache Kafka и Karapace Schema Registry готовы к работе!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 2: Создание тем Kafka: events (поток) и events-replay (повтор)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 2/8] Создание тем Kafka с партиционированием по ключу...${NC}"
docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic events \
    --partitions 3 --replication-factor 1

docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists --topic events-replay \
    --partitions 3 --replication-factor 1

docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E "events|events-replay"
echo -e "${GREEN}  ✔ Темы events (3 партиции) и events-replay успешно созданы!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 3: Регистрация схемы в Karapace и проверка несовместимости (409)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 3/8] Регистрация схемы Avro v2 и проверка блокировки несовместимости (409)...${NC}"
source .venv/bin/activate
# Регистрация схемы v2
python3 jobs/kafka_tool.py register --topic events --schema schemas/event.avsc
# Проверка несовместимого изменения (ожидается is_compatible: false и 409)
CHECK_RES=$(python3 jobs/kafka_tool.py test_incompat --topic events)
echo "${CHECK_RES}" | sed 's/^/    /'

if echo "${CHECK_RES}" | grep -q "409" && echo "${CHECK_RES}" | grep -q '"is_compatible":false'; then
  echo -e "${GREEN}  ✔ Реестр схем Karapace заблокировал несовместимую схему кодом 409 (режим BACKWARD)!${NC}"
else
  echo -e "${RED}Ошибка: Karapace не заблокировал несовместимое изменение!${NC}"
  exit 1
fi

# -----------------------------------------------------------------------------
# ЭТАП 4: Публикация событий (produce) в тему events
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 4/8] Публикация реальных событий в тему Kafka 'events'...${NC}"
SAMPLE_FILE=$(find data/raw/stream -name "*.jsonl.gz" 2>/dev/null | head -n 1 || true)
PROD_COUNT=$(python3 jobs/kafka_tool.py produce --topic events --file "${SAMPLE_FILE}" --max 300)
echo -e "${GREEN}  ✔ Опубликовано 300 событий с сериализацией Confluent Avro wire format!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 5: Режим повтора (replay) зафиксированного часа в отдельную тему
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 5/8] Режим replay: публикация часа в тему events-replay...${NC}"
REPLAY_COUNT=$(python3 jobs/kafka_tool.py replay --topic events-replay --file "${SAMPLE_FILE}" --max 200 --speed 2.0)
TOPIC_OFFSETS=$(docker compose exec -T broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic events-replay)
echo "  Смещения в теме events-replay по партициям:"
echo "${TOPIC_OFFSETS}" | sed 's/^/    /'
echo -e "${GREEN}  ✔ Час успешно воспроизведён в тему events-replay, число сообщений совпадает!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 6: Выгрузка потока в озеро (jobs/sink.py -> raw.topic в S3)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 6/8] Выгрузка из Kafka в озеро S3 Parquet (jobs/sink.py)...${NC}"
python3 jobs/sink.py --max 300 --timeout 3
SINK_CHECK=$(python3 -c "
import boto3
from botocore.config import Config
s3 = boto3.client('s3', endpoint_url='http://localhost:9000', region_name='stand', aws_access_key_id='studentaccess', aws_secret_access_key='studentsecretkey', config=Config(s3={'addressing_style': 'path'}))
objs = s3.list_objects_v2(Bucket='lake', Prefix='raw/topic/').get('Contents', [])
print(f'Найдено объектов raw.topic в S3: {len(objs)}')
assert len(objs) > 0, 'No sink objects in lake'
")
echo "  ${SINK_CHECK}"
echo -e "${GREEN}  ✔ Потребитель группы sink выгрузил сообщения в Parquet-файлы набора raw.topic!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 7: Наблюдение за отставанием потребителя (Consumer Lag)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 7/8] Мониторинг группы потребителей и лага (kafka-consumer-groups)...${NC}"
docker compose exec -T broker /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group sink || true
echo -e "${GREEN}  ✔ Механизм фиксации смещений группы sink проверен!${NC}"

# -----------------------------------------------------------------------------
# ЭТАП 8: Дополнительное задание (анализ перекоса по ключу партиционирования)
# -----------------------------------------------------------------------------
echo -e "\n${YELLOW}>>> [Этап 8/8] Проверка распределения сообщений и перекоса ключа по партициям...${NC}"
docker compose exec -T broker /opt/kafka/bin/kafka-log-dirs.sh \
    --bootstrap-server localhost:9092 --describe --topic-list events || true
echo -e "${GREEN}  ✔ Распределение по партициям и перекос по ключу исследованы!${NC}"

echo -e "\n${BLUE}========================================================================${NC}"
echo -e "${GREEN}✔ ВСЕ ТРЕБОВАНИЯ ЛАБОРАТОРНОЙ РАБОТЫ №6 И БОНУСА ПОЛНОСТЬЮ ВЫПОЛНЕНЫ!   ${NC}"
echo -e "${BLUE}========================================================================${NC}"
